SELECT w.id, w.event_type, w.duration_sec, w.event_time FROM web_events w WHERE w.duration_sec = (SELECT MIN(w2.duration_sec) FROM web_events w2 WHERE w2.event_type = w.event_type);
