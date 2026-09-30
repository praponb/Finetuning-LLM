SELECT s.id, s.country
FROM sessions s
WHERE s.customer_id IS NOT NULL AND s.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(name) >= 11)
ORDER BY country DESC
