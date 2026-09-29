SELECT
    id,
    page,
    duration_sec,
    session_id,
    FIRST_VALUE(session_id) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS first_session_id
FROM web_events
