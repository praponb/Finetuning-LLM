SELECT id, quantity
FROM order_items
WHERE order_id IN (19, 29, 31)
ORDER BY quantity DESC;
