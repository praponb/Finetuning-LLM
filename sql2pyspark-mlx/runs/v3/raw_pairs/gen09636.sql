SELECT a.id, a.event_type, a.duration_sec FROM web_events a WHERE a.duration_sec = (SELECT MIN(b.duration_sec) FROM web_events b WHERE b.event_type = a.event_type)
