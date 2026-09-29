SELECT id, session_id, duration_sec
FROM (
    SELECT
        id,
        session_id,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
    WHERE event_type != 'search'
) AS r
WHERE row_num <= 2
ORDER BY id DESC;
