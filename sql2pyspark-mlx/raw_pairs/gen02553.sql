SELECT city AS label FROM stores WHERE city IS NOT NULL UNION ALL SELECT event_type AS label FROM web_events WHERE event_type IS NOT NULL
