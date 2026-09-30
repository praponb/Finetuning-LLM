SELECT id, page, event_time, DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time) dense_rnk, RANK() OVER (PARTITION BY page ORDER BY event_time) rnk FROM web_events
