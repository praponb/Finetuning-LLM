SELECT i.quantity, s.city, s.store_name FROM inventory i INNER JOIN stores s ON i.store_id = s.id ORDER BY quantity, s.city
