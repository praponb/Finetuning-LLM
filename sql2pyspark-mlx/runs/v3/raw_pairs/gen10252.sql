SELECT
    s.id,
    s.browser,
    s.customer_id,
    s.device
FROM sessions s
WHERE s.device = 'desktop' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec != 412);
