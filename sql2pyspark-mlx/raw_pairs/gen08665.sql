SELECT
    id,
    event_time,
    session_id,
    page,
    TO_DATE(event_time) AS event_time_date
FROM web_events;
