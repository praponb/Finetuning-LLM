SELECT s.browser, MAX(we.event_time) AS latest_event_time, COUNT(CASE WHEN we.page = '/checkout' THEN 1 END) AS checkout_count
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
GROUP BY s.browser
ORDER BY latest_event_time
