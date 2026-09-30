SELECT
    order_items.id,
    order_items.unit_price,
    order_items.product_id,
    order_items.quantity
FROM order_items
WHERE order_items.product_id NOT IN (SELECT id FROM products WHERE stock_qty BETWEEN 232 AND 376);
