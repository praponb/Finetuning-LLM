SELECT id, event_type, event_time, page FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS pos FROM web_events) AS r WHERE pos <= 3 ORDER BY page DESC
