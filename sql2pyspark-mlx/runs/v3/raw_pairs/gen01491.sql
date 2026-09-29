SELECT product_id, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE NOT (quantity = 4 AND order_id IN (15, 24))
GROUP BY product_id
ORDER BY min_unit_price ASC, product_id DESC
LIMIT 1
