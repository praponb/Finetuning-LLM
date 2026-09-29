select id, duration_sec, session_id from web_events where duration_sec > (select max(duration_sec) from web_events);
