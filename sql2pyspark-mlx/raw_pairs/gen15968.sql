SELECT s.id, s.sqft, s.city FROM stores s WHERE s.opened_date < '2024-12-08' AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id IN (1, 2, 3, 6));
