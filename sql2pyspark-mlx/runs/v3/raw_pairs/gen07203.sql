SELECT page, event_type, SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS click_count
FROM web_events
WHERE session_id IN (7, 18, 37, 40) OR page <> '/product'
GROUP BY page, event_type
HAVING click_count > 39
ORDER BY event_type, click_count DESC, page
LIMIT 50
