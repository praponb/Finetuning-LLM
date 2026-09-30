SELECT s.id, s.started_at
FROM sessions s
WHERE s.country IN ('UK', 'TH', 'US') AND s.customer_id NOT IN (SELECT id FROM customers WHERE LOWER(name) LIKE '%ran%')
ORDER BY id
