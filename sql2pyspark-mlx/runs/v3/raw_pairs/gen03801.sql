SELECT session_id, YEAR(event_time) AS event_year, COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count
FROM web_events
WHERE page NOT IN ('/product', '/home')
GROUP BY session_id, YEAR(event_time)
HAVING checkout_count < 2
