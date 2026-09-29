SELECT i.quantity, i.store_id, s.sqft FROM inventory i LEFT JOIN stores s ON s.id = i.store_id
