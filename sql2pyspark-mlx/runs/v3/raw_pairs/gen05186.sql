SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price, LEAST(quantity, unit_price) AS least_quantity_unit_price
FROM order_items
ORDER BY order_id, quantity
