SELECT s.store_name, s.city, s.sqft, i.quantity, i.last_restock, CASE WHEN s.region = 'East' THEN 1 ELSE 0 END is_east FROM stores s LEFT JOIN inventory i ON i.store_id = s.id ORDER BY city;
