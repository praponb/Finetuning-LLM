select a.id, a.session_id, a.duration_sec from web_events a where a.duration_sec = (select min(b.duration_sec) from web_events b where b.session_id = a.session_id);
