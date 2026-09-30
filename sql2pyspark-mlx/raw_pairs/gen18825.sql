SELECT s.id, s.browser, s.started_at
FROM sessions s
WHERE s.country = 'SG' AND s.id IN (SELECT session_id FROM web_events WHERE event_type IN ('click', 'search', 'add_to_cart'))
ORDER BY browser
