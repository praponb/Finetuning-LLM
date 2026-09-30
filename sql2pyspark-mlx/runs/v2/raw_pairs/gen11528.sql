SELECT id, page, event_time, event_type FROM (SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rn FROM web_events) AS ranked WHERE rn <= 2 ORDER BY event_type
