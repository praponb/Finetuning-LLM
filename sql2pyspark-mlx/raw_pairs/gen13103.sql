SELECT id, quantity
FROM order_items
WHERE order_id IN (20, 22) OR quantity BETWEEN 4 AND 6;
