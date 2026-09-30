SELECT o.id, o.store_id FROM orders o WHERE o.id IN (SELECT order_id FROM order_items WHERE product_id IS NOT NULL)
