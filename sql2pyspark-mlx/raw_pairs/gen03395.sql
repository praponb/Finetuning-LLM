SELECT id, unit_price, ROUND(unit_price / quantity, 2) AS line_total
FROM order_items
WHERE order_id IN (20, 34) OR order_id = 24
