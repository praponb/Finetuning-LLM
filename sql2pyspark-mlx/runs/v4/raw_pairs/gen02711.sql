SELECT
    event_type,
    COUNT_IF(event_type = 'purchase') AS purchase_events,
    AVG(duration_sec) FILTER (WHERE event_type = 'page_view') AS page_view_avg_duration_sec
FROM web_events
GROUP BY event_type;
