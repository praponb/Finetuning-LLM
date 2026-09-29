SELECT id, order_id, LEAST(unit_price, quantity) AS least_unit_price_quantity
FROM order_items
WHERE order_id IN (21, 37) AND unit_price >= 210
