SELECT s.id, s.browser, COUNT(we.id) AS web_event_count
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE s.customer_id IN (23, 36)
GROUP BY s.id, s.browser
ORDER BY id DESC
