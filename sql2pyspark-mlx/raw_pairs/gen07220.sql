SELECT
    we.id,
    we.duration_sec,
    s.customer_id,
    s.device
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
WHERE we.session_id IN (8, 40) AND we.session_id IN (14, 20, 35)
ORDER BY customer_id
