SELECT s.region, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, COUNT(i.quantity) AS quantity_count
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
RIGHT OUTER JOIN stores s ON i.store_id = s.id
GROUP BY s.region
