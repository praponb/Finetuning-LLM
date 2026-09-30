SELECT product_id, SUM(quantity) AS total_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
GROUP BY 1
HAVING SUM(quantity) < 24
ORDER BY avg_unit_price DESC, product_id
LIMIT 10
