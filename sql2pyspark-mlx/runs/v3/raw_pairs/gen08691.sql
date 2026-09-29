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
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS rn
    FROM web_events
) AS t
WHERE rn = 1
ORDER BY id DESC
