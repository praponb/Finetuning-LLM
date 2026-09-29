SELECT id, order_id
FROM order_items
WHERE product_id IS NOT NULL OR unit_price > 190
