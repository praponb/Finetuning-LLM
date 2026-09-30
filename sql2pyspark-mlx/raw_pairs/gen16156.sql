SELECT i.id, i.quantity, s.region, s.city FROM inventory i INNER JOIN stores s ON i.store_id = s.id;
