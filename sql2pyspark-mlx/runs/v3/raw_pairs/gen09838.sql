SELECT
    id,
    event_time,
    page,
    session_id,
    CASE WHEN page = '/product' THEN 1 ELSE 0 END AS is_product,
    YEAR(event_time) AS event_time_year
FROM web_events
