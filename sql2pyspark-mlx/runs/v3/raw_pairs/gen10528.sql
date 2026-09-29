SELECT c.id, c.city, c.age
FROM customers c
WHERE c.signup_date > '2023-11-02' AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.order_date < '2025-09-01')
ORDER BY c.age, id
LIMIT 5;
