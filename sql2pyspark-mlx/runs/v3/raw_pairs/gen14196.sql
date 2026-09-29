SELECT
    TO_DATE(event_time) AS event_day,
    session_id,
    page,
    SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count
FROM web_events
GROUP BY TO_DATE(event_time), session_id, page
ORDER BY session_id DESC
