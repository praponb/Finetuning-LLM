SELECT c.id, c.name, c.signup_date
FROM customers c
WHERE c.id IN (SELECT customer_id FROM orders)
ORDER BY name ASC;
