SELECT order_items.id, order_items.unit_price, order_items.quantity
FROM order_items
WHERE order_items.order_id IN (SELECT id FROM orders WHERE order_date > '2025-09-28')
ORDER BY order_items.unit_price DESC, 3 DESC;
