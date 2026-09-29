SELECT order_id, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
FROM order_items
WHERE quantity > 5
ORDER BY greatest_quantity_unit_price DESC
