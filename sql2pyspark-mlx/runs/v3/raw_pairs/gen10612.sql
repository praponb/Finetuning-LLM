SELECT status
FROM orders
WHERE store_id IS NOT NULL
EXCEPT
SELECT status
FROM orders
WHERE shipping_fee >= 26
ORDER BY status
