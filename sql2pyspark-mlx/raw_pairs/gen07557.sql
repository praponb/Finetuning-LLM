SELECT i.quantity, i.last_restock, s.sqft, s.region FROM inventory i INNER JOIN stores s ON s.id = i.store_id WHERE s.store_name LIKE '%Tow%' OR i.store_id IN (2, 4, 5) ORDER BY quantity DESC
