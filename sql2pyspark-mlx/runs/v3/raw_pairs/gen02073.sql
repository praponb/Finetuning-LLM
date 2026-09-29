SELECT s.city, COUNT(i.id) AS inventory_count FROM stores s LEFT JOIN inventory i ON i.store_id = s.id WHERE i.product_id = 8 GROUP BY s.city
