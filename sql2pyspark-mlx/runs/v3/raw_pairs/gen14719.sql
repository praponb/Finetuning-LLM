SELECT
    order_items.id,
    order_items.order_id,
    order_items.product_id,
    order_items.quantity
FROM order_items
WHERE order_items.order_id IN (32, 37, 39) AND order_items.order_id IN (SELECT id FROM orders WHERE amount >= 1420);
