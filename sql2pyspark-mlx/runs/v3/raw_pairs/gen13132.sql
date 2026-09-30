SELECT w.id, w.event_type, w.duration_sec FROM web_events w WHERE w.duration_sec < (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.event_type = w.event_type)
