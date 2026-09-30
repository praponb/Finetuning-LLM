SELECT s.city, MAX(i.quantity) AS max_quantity FROM inventory i LEFT JOIN stores s ON s.id = i.store_id GROUP BY s.city HAVING MAX(i.quantity) >= 140 ORDER BY city, max_quantity DESC
