select w.id, w.session_id, w.duration_sec from web_events w where w.duration_sec > (select avg(w2.duration_sec) from web_events w2 where w2.session_id = w.session_id);
