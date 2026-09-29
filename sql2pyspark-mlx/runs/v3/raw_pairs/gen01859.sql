SELECT order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
WHERE product_id IN (2, 14)
GROUP BY order_id
ORDER BY avg_unit_price DESC, stddev_quantity, order_id
LIMIT 5
