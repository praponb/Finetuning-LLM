SELECT
    order_items.id,
    order_items.product_id,
    order_items.unit_price,
    order_items.order_id
FROM order_items
WHERE order_items.quantity = 8 AND order_items.order_id IN (SELECT id FROM orders WHERE status IN ('RETURNED', 'CANCELLED'))
ORDER BY unit_price DESC;
