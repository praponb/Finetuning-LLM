SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
WHERE order_id = 37 OR product_id IS NOT NULL
GROUP BY 1
HAVING COUNT(DISTINCT unit_price) > 3
ORDER BY stddev_unit_price;
