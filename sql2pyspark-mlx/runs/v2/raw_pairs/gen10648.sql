SELECT * FROM (SELECT YEAR(event_time) AS event_year, event_type, duration_sec FROM web_events)
PIVOT (SUM(duration_sec) FOR event_type IN ('purchase', 'page_view', 'add_to_cart'))
ORDER BY event_year
