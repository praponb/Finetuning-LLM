SELECT duration_sec, event_time, ROUND(duration_sec * 1.15, 2) AS duration_sec_adjusted
FROM web_events
WHERE event_type = 'page_view'
ORDER BY duration_sec DESC, event_time;
