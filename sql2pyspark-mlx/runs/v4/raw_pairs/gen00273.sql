SELECT
    s.id,
    s.started_at,
    s.device,
    s.customer_id
FROM sessions s
WHERE s.browser != 'Edge' AND s.customer_id IN (SELECT id FROM customers WHERE name IS NOT NULL)
ORDER BY id
LIMIT 3
