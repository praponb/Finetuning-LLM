SELECT id, event_type, event_time, DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time) AS event_time_dense_rank FROM web_events WHERE TO_DATE(event_time) < '2024-07-11'
