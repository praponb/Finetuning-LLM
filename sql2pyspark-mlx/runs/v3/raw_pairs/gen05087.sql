SELECT status, 'order' AS source
FROM orders
WHERE store_id IN (16, 20, 32)
UNION ALL
SELECT status, 'payment' AS source
FROM payments;
