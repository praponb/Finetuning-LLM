WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rnk FROM web_events) SELECT id, page, event_time, session_id FROM ranked WHERE rnk <= 3
