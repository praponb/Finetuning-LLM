SELECT
    session_id,
    event_type,
    page,
    CAST(event_time AS DATE) AS event_time_day
FROM web_events;
