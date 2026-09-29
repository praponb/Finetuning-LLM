SELECT
    page,
    event_type,
    session_id,
    SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY page, event_type, session_id;
