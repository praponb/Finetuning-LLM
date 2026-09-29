SELECT ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search FROM web_events WHERE page = '/cart'
