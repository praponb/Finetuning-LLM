SELECT unit_price AS value
FROM order_items
INTERSECT
SELECT shipping_fee AS value
FROM orders
ORDER BY value;
