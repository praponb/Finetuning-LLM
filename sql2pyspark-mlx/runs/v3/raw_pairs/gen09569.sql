SELECT
    order_items.id,
    order_items.order_id,
    order_items.product_id,
    order_items.quantity
FROM order_items
WHERE order_items.quantity <= 7 AND order_items.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL)
