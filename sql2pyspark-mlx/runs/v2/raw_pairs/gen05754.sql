SELECT s.id, s.browser, s.started_at
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE 'Hen%');
