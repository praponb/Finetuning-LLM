SELECT id, customer_id, browser
FROM sessions
WHERE started_at >= '2025-10-01'
EXCEPT
SELECT id, customer_id, browser
FROM sessions
WHERE device IN ('desktop', 'mobile');
