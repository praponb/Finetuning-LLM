SELECT
    session_id,
    event_type,
    page,
    COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE TO_DATE(event_time) >= '2023-05-10' OR event_type = 'search';
