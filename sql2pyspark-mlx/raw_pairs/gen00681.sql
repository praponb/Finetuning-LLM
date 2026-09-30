SELECT id, page, duration_sec
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS rn
    FROM web_events
) AS t
WHERE rn <= 3
