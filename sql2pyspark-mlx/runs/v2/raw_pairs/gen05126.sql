SELECT status, 'order' AS source
FROM orders
WHERE shipping_fee BETWEEN 18 AND 35
UNION ALL
SELECT status, 'payment' AS source
FROM payments;
