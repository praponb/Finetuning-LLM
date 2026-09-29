SELECT id, unit_price, LEAST(quantity, unit_price) AS least_quantity_unit_price
FROM order_items
WHERE unit_price < 220 AND unit_price IS NOT NULL AND quantity = 6
