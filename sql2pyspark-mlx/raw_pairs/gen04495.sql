SELECT shipping_fee
FROM orders
WHERE shipping_fee IS NOT NULL
EXCEPT
SELECT cost
FROM shipments
WHERE cost IS NOT NULL;
