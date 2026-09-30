WITH Numbered AS (
    SELECT
        id,
        event_type,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
    WHERE duration_sec < 222
)
SELECT *
FROM Numbered
WHERE pos = 1
ORDER BY pos DESC, id;
