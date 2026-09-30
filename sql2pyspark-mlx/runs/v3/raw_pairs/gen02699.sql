SELECT shipping_fee, 'order' AS source
FROM orders
UNION
SELECT unit_price, 'order_item' AS source
FROM order_items
WHERE quantity != 5;
