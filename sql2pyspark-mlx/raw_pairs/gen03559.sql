SELECT i.quantity, s.sqft, s.city
FROM inventory i
JOIN stores s ON s.id = i.store_id;
