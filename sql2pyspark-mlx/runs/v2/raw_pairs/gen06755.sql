SELECT
    id,
    event_type,
    duration_sec,
    page
FROM (
    SELECT
        id,
        event_type,
        duration_sec,
        page,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
) AS ranked
WHERE row_num <= 5
ORDER BY id
