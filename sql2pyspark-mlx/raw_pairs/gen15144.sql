SELECT id, quantity, unit_price
FROM order_items
WHERE order_id IN (7, 21, 23) AND order_id IN (3, 25, 32, 33) AND unit_price > 340
ORDER BY unit_price DESC, id DESC
