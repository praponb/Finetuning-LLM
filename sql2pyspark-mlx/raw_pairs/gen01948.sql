SELECT
    id,
    page,
    duration_sec,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS rn
FROM web_events
ORDER BY page ASC, duration_sec DESC
