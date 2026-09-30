SELECT amount
FROM orders
EXCEPT
SELECT unit_price
FROM order_items
ORDER BY amount
