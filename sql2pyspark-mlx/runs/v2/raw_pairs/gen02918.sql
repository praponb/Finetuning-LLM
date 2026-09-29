SELECT id, session_id, event_time, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS dense_rnk FROM web_events WHERE TO_DATE(event_time) >= '2024-03-01'
