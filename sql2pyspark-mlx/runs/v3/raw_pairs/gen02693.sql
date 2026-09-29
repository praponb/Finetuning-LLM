SELECT id, order_id, unit_price
FROM order_items
WHERE order_id IN (23, 25, 26, 29) AND unit_price < 240
