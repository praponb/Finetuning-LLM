SELECT id, order_id, unit_price
FROM order_items
WHERE order_id IN (18, 21, 24, 39) AND unit_price >= 310 AND unit_price > 360
ORDER BY id DESC
