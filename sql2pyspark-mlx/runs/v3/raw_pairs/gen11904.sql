SELECT shipping_fee AS value
FROM orders
UNION ALL
SELECT price AS value
FROM products
WHERE cost BETWEEN 280 AND 490
ORDER BY value;
