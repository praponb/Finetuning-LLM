SELECT *, LEAST(quantity, unit_price) AS least_quantity_unit_price
FROM order_items
WHERE order_id IN (1, 19, 23, 30)
