SELECT duration_sec FROM web_events WHERE duration_sec <> 346 UNION SELECT duration_sec FROM web_events WHERE event_type <> 'purchase';
