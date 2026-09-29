SELECT s.city, SUM(i.quantity) AS total_quantity FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT OUTER JOIN stores s ON s.id = i.store_id GROUP BY s.city ORDER BY s.city;
