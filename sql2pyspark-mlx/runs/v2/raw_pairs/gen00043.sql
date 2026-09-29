SELECT *, CASE WHEN page IN ('/cart', '/checkout') THEN 'Priority' ELSE 'Other' END AS page_group, DATE_TRUNC('MONTH', event_time) AS event_time_period
FROM web_events
ORDER BY event_time_period DESC
