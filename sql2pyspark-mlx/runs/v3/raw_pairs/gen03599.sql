SELECT event_type FROM web_events WHERE event_type = 'add_to_cart' UNION ALL SELECT event_type FROM web_events WHERE HOUR(event_time) BETWEEN 8 AND 12 ORDER BY event_type DESC;
