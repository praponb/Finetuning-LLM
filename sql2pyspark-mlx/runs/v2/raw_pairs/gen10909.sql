SELECT *
FROM (
    SELECT
        id,
        session_id,
        duration_sec,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS pos
    FROM web_events
) AS ranked
WHERE pos <= 5
ORDER BY session_id DESC;
