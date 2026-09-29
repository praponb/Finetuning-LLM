SELECT s.region, COUNT(*) AS total_count, COUNT(*) AS row_count FROM inventory i RIGHT JOIN stores s ON i.store_id = s.id INNER JOIN products p ON p.id = i.product_id GROUP BY s.region;
