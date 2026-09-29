SELECT s.city, i.store_id, i.product_id, i.quantity FROM stores s LEFT JOIN inventory i ON s.id = i.store_id WHERE i.product_id IN (4, 8, 9) OR i.product_id IN (1, 9);
