SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events)
PIVOT (MAX(duration_sec) FOR event_type IN ('click', 'purchase', 'search'))
ORDER BY page
