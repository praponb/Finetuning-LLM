SELECT order_items.id, order_items.product_id
FROM order_items
WHERE order_items.order_id IN (SELECT id FROM orders WHERE channel NOT IN ('store', 'partner', 'web'));
