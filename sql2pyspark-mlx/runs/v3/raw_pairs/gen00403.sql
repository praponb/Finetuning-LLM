SELECT shipping_fee
FROM orders
UNION
SELECT cost
FROM shipments;
