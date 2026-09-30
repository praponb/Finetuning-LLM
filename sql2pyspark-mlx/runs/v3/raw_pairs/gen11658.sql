WITH Numbered AS (
    SELECT
        id,
        page,
        event_time,
        RANK() OVER (PARTITION BY page ORDER BY event_time DESC) pos
    FROM web_events
)
SELECT *
FROM Numbered
WHERE pos <= 2
ORDER BY id DESC;
