SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE NOT (order_id IN (8, 11, 35) AND unit_price <= 250 AND product_id IN (10, 16, 26, 28))
GROUP BY product_id
ORDER BY min_unit_price DESC
