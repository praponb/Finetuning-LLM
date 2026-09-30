SELECT
    id,
    event_type,
    page,
    session_id,
    duration_sec,
    COUNT(*) OVER (PARTITION BY event_type, page) AS event_type_count,
    MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
FROM web_events
ORDER BY session_id_max_duration_sec DESC, event_type DESC;
