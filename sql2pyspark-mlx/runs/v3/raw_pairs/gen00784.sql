SELECT id, page, duration_sec, RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) AS rnk FROM web_events WHERE page IS NOT NULL
