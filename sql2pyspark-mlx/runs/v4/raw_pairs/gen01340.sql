SELECT order_id, product_id
FROM order_items
EXCEPT DISTINCT
SELECT order_id, product_id
FROM order_items
WHERE quantity > 7;
