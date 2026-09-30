SELECT s.sqft, s.city, s.opened_date, i.store_id, i.product_id FROM stores s LEFT JOIN inventory i ON s.id = i.store_id WHERE i.quantity IS NOT NULL
