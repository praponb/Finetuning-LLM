SELECT w.id, w.session_id, w.duration_sec, w.event_type FROM web_events w WHERE w.duration_sec = (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = w.session_id);
