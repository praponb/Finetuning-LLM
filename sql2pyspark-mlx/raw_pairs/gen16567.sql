SELECT order_items.id, order_items.order_id, order_items.product_id
FROM order_items
WHERE order_items.product_id NOT IN (SELECT id FROM products WHERE supplier_id IN (19, 22, 30, 37))
