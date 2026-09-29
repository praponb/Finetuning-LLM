WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS pos FROM web_events) SELECT id, page, duration_sec FROM numbered WHERE pos <= 2
