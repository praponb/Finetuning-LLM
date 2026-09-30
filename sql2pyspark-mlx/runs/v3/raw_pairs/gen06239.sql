SELECT amount
FROM orders
WHERE discount < 0.24
UNION ALL
SELECT unit_price
FROM order_items
WHERE unit_price >= 200;
