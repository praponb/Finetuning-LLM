SELECT s.store_name, i.store_id FROM stores s LEFT JOIN inventory i ON i.store_id = s.id ORDER BY s.store_name
