SELECT s.id, s.device, s.browser, s.customer_id FROM sessions s WHERE s.browser = 'Safari' AND s.id IN (SELECT session_id FROM web_events) ORDER BY browser DESC, s.customer_id DESC, id LIMIT 5;
