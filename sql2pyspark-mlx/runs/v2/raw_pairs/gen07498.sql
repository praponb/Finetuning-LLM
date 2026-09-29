SELECT s.city, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, MAX(i.quantity) AS max_quantity
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
INNER JOIN products p ON p.id = i.product_id
GROUP BY s.city
HAVING MAX(i.quantity) > 70
ORDER BY stddev_quantity, city
LIMIT 5;
