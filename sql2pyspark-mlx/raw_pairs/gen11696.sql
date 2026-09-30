SELECT MAX(event_time) AS latest_event_time, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count FROM web_events ORDER BY home_count DESC
