SELECT s.id, s.country
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) > 7);
