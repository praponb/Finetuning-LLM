SELECT s.id, s.customer_id, s.browser, s.country FROM sessions s WHERE s.started_at < '2024-02-16' AND s.id IN (SELECT session_id FROM web_events)
