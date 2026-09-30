SELECT SUM(duration_sec) AS total_duration_sec, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
FROM web_events
WHERE event_time >= '2023-10-01' OR event_type IN ('search', 'purchase', 'click')
