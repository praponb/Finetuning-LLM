SELECT
    s.id,
    s.customer_id,
    s.device,
    s.country
FROM sessions s
WHERE TO_DATE(s.started_at) < '2025-03-01' AND s.id IN (SELECT session_id FROM web_events)
