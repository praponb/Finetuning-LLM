SELECT status
FROM orders
WHERE store_id = 34
UNION
SELECT status
FROM payments
WHERE amount >= 860
