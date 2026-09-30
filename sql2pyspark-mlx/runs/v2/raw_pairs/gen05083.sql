SELECT id, customer_id
FROM orders
WHERE customer_id IN (1, 8, 21, 39)
EXCEPT
SELECT id, customer_id
FROM orders
WHERE customer_id = 34
ORDER BY id;
