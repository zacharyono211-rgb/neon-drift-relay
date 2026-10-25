import http from 'node:http';
import { randomInt } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';

// Run one replica: all participants in a room must reach this process.
export function createRelay() {
  const rooms = new Map();
  const server = http.createServer((req, res) => {
    res.writeHead(req.url === '/health' ? 200 : 404, {'Content-Type':'application/json'});
    res.end(JSON.stringify(req.url === '/health' ? {ok:true,service:'neon-drift-relay'} : {error:'Not found'}));
  });
  const wss = new WebSocketServer({server, maxPayload:1024*1024, perMessageDeflate:false});
  const hostMethods = new Set(['_receive_lobby','_assigned','_start_run','_snapshot_chunk','_open_lobby','_version_rejected']);
  const clientMethods = new Set(['_verify_version','_register_name','_input_update','_choose_upgrade','_menu']);
  const send = (ws, value) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(value)); };
  function disconnect(ws) {
    const room = rooms.get(ws.code);
    if (!room || room.players.get(ws.id) !== ws) return;
    room.players.delete(ws.id);
    if (ws.id === 1) {
      rooms.delete(ws.code);
      for (const other of room.players.values()) { send(other,{type:'closed',message:'The host left the lobby.'}); other.close(1000); }
    } else send(room.players.get(1),{type:'left',id:ws.id});
  }
  wss.on('connection', ws => {
    ws.alive=true; ws.started=Date.now(); ws.window=Date.now(); ws.messages=0; ws.bytes=0;
    ws.on('pong',()=>ws.alive=true);
    ws.on('close',()=>disconnect(ws));
    ws.on('error',()=>{});
    ws.on('message', raw => {
      const now=Date.now();
      if (now-ws.window>1000) { ws.window=now; ws.messages=0; ws.bytes=0; }
      ws.messages++; ws.bytes+=raw.length;
      if (ws.messages>(ws.id===1?3000:100) || ws.bytes>(ws.id===1?8*1024*1024:128*1024)) { ws.close(1008,'Rate limit'); return; }
      let msg; try { msg=JSON.parse(raw.toString()); } catch { ws.close(1008,'Invalid message'); return; }
      if (!ws.code) {
        if (!/^[\d]+\.[\d]+\.[\d]+$/.test(msg.version || '')) { send(ws,{type:'error',message:'Update your game before joining.'}); return; }
        let room;
        if (msg.type==='host') {
          if (rooms.size>=9000) { send(ws,{type:'error',message:'All lobby codes are busy. Try again soon.'}); return; }
          let code; do { code=String(randomInt(1000,10000)); } while(rooms.has(code));
          room={code,version:msg.version,players:new Map(),started:false}; rooms.set(code,room); ws.id=1;
        } else if (msg.type==='join' && /^\d{4}$/.test(msg.code || '')) {
          room=rooms.get(msg.code);
          if (!room) { send(ws,{type:'error',message:'Lobby code not found. Ask the host for the current code.'}); return; }
          if (room.started) { send(ws,{type:'error',message:'This game has already started.'}); return; }
          if (room.version!==msg.version) { send(ws,{type:'error',message:`Version mismatch. Host uses v${room.version}.`}); return; }
          ws.id=[2,3,4].find(id=>!room.players.has(id));
          if (!ws.id) { send(ws,{type:'error',message:'Lobby full (four players maximum).'}); return; }
        } else { send(ws,{type:'error',message:'Enter a four-digit lobby code.'}); return; }
        ws.code=room.code; room.players.set(ws.id,ws);
        send(ws,{type:'welcome',id:ws.id,code:room.code});
        if (ws.id!==1) send(room.players.get(1),{type:'joined',id:ws.id});
        return;
      }
      const room=rooms.get(ws.code);
      if (!room || msg.type!=='packet' || typeof msg.data!=='string' || msg.data.length>800000) return;
      if (!(ws.id===1?hostMethods:clientMethods).has(msg.method)) return;
      if (ws.id!==1 && msg.to!==1) return;
      if (ws.id===1 && msg.method==='_start_run') room.started=true;
      const packet={type:'packet',from:ws.id,method:msg.method,data:msg.data};
      for (const [id, other] of room.players) {
        if (id===ws.id || (msg.to!==0 && msg.to!==id)) continue;
        // Drop stale snapshot chunks under congestion; never queue unlimited state.
        if (other.bufferedAmount>2*1024*1024) { if (msg.method==='_snapshot_chunk') continue; other.close(1013,'Connection too slow'); continue; }
        send(other,packet);
      }
    });
  });
  const heartbeat=setInterval(()=>{
    for(const ws of wss.clients) {
      if (!ws.alive || (!ws.code && Date.now()-ws.started>15000)) { ws.terminate(); continue; }
      ws.alive=false; ws.ping();
    }
  },10000);
  heartbeat.unref();
  return {server,rooms,close:()=>{clearInterval(heartbeat);for(const ws of wss.clients)ws.terminate();wss.close();server.close();}};
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const relay=createRelay(); relay.server.listen(Number(process.env.PORT || 8080),'0.0.0.0',()=>console.log('Neon Drift relay listening'));
}
