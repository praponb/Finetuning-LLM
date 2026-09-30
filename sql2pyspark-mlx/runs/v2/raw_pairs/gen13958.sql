SELECT
    id,
    session_id,
    duration_sec,
    MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
FROM web_events
WHERE session_id = 12;
