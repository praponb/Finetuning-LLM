SELECT id, page, duration_sec, NTILE(10) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS quartile FROM web_events ORDER BY page DESC;
