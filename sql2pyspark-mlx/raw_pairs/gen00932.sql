SELECT s.id, s.device, s.customer_id
FROM sessions s
WHERE s.browser IN ('Edge', 'Firefox', 'Chrome') AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id);
