select s.id, s.device from sessions s where s.started_at < '2025-10-01' and exists (select 1 from web_events we where we.session_id = s.id and we.event_time >= '2024-04-01');
