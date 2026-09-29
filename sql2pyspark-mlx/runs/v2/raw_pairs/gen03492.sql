SELECT order_items.id, order_items.unit_price
FROM order_items
WHERE order_items.order_id NOT IN (SELECT id FROM orders WHERE discount BETWEEN 0.24 AND 0.33)
ORDER BY id DESC, unit_price
LIMIT 3
