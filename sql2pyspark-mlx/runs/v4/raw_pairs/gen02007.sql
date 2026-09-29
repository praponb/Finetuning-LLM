SELECT id, event_type, event_time FROM web_events WHERE event_time >= '2024-03-02' ORDER BY id DESC, event_time LIMIT 20;
