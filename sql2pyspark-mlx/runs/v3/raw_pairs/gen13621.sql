SELECT w.id, w.event_type, w.duration_sec FROM web_events w WHERE w.duration_sec = (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.event_type = w.event_type) ORDER BY w.id, w.duration_sec;
