SELECT *
FROM (
    SELECT
        id,
        event_type,
        duration_sec,
        page,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
) AS t
WHERE pos <= 1;
