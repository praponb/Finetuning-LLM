SELECT s.id, s.device, s.started_at
FROM sessions s
WHERE s.country IN ('SG', 'JP', 'TH') AND s.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL)
ORDER BY s.id DESC;
