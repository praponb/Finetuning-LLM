SELECT MAX(quantity) AS max_quantity, SUM(unit_price) AS total_unit_price
FROM order_items
ORDER BY total_unit_price DESC
