SELECT status
FROM orders
WHERE shipping_fee >= 25
UNION
SELECT status
FROM payments
ORDER BY status
