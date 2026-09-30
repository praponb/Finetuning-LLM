SELECT s.city, MAX(i.quantity) AS max_quantity FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id GROUP BY s.city HAVING MAX(i.quantity) > 200 ORDER BY max_quantity DESC, city LIMIT 50;
