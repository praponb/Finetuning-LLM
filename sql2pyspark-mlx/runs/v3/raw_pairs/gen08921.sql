SELECT product_id, COUNT(quantity) AS quantity_count, SUM(unit_price) AS total_unit_price
FROM order_items
GROUP BY product_id
HAVING quantity_count < 5
ORDER BY total_unit_price DESC, product_id
LIMIT 5
