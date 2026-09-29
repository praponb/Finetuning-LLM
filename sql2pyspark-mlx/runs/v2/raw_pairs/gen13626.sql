SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events)
PIVOT (COUNT(duration_sec) FOR event_type IN ('click', 'search', 'page_view', 'purchase'))
ORDER BY page
