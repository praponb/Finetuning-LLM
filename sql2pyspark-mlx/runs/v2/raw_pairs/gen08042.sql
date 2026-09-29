SELECT ss.product_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units >= 37
GROUP BY ss.product_id
