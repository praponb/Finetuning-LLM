SELECT
    id,
    event_type,
    duration_sec,
    event_time
FROM (
    SELECT
        id,
        event_type,
        duration_sec,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
) AS r
WHERE row_num <= 1
ORDER BY duration_sec;
