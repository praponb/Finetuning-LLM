SELECT
    id,
    session_id,
    duration_sec,
    NTILE(2) OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS bucket
FROM web_events
ORDER BY session_id DESC, duration_sec
