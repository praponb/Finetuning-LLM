SELECT id, page, duration_sec FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) pos FROM web_events) t WHERE pos <= 3
