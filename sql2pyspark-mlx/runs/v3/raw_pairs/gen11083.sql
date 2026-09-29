WITH numbered AS (
    SELECT
        id,
        page,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS pos
    FROM web_events
    WHERE page NOT IN ('/search', '/home')
)
SELECT id, page, event_time
FROM numbered
WHERE pos = 1
ORDER BY 3 DESC;
