SELECT
    event_type,
    session_id,
    event_time,
    CAST(duration_sec AS STRING) AS duration_sec_str,
    FLOOR(duration_sec) AS duration_sec_whole
FROM web_events
WHERE event_type NOT IN ('page_view', 'search');
