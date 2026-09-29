SELECT
    id,
    event_type,
    page,
    UPPER(page) AS page_upper,
    DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
FROM web_events;
