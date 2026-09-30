SELECT product_id, order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
WHERE (quantity BETWEEN 4 AND 6 AND order_id IN (1, 8, 30, 40)) OR order_id IN (23, 31)
GROUP BY 1, 2
