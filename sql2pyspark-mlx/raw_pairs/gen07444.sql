SELECT price
FROM products
INTERSECT
SELECT shipping_fee
FROM orders
WHERE amount > 900
