SELECT
    id,
    page,
    event_time,
    session_id
FROM (
    SELECT
        id,
        page,
        event_time,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) row_num
    FROM web_events
) ranked
WHERE row_num = 1
ORDER BY page DESC;
