SELECT amount, 'order' AS source
FROM orders
UNION
SELECT unit_price, 'order_item' AS source
FROM order_items
WHERE order_id IN (20, 21, 26, 36);
