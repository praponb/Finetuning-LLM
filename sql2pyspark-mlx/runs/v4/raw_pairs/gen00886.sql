SELECT unit_price
FROM order_items
EXCEPT
SELECT amount AS unit_price
FROM orders
