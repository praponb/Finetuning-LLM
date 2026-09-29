SELECT s.id, s.device, s.started_at
FROM sessions s
WHERE s.country IN ('UK', 'US') AND s.customer_id IN (SELECT id FROM customers WHERE email IS NULL)
