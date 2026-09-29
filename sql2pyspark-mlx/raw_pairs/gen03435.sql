SELECT s.city, s.region, ROUND(AVG(i.quantity), 2) AS avg_quantity FROM inventory i INNER JOIN stores s ON s.id = i.store_id GROUP BY s.city, s.region ORDER BY avg_quantity DESC, s.city;
