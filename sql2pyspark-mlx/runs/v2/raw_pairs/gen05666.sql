SELECT
    s.id,
    s.customer_id,
    s.country,
    s.browser
FROM sessions s
WHERE s.id IN (SELECT session_id FROM web_events WHERE session_id IN (8, 13))
