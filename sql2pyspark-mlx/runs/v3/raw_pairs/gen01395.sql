SELECT shipping_fee
FROM orders
INTERSECT
SELECT cost
FROM products
ORDER BY shipping_fee;
