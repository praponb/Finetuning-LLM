SELECT id, page, event_time
FROM (
    SELECT
        id,
        page,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS rnk
    FROM web_events
) AS ranked
WHERE rnk <= 5
ORDER BY id DESC;
