SELECT MAX(event_time) AS latest_event_time, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
FROM web_events
WHERE duration_sec >= 296;
