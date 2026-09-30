SELECT i.id, i.quantity, s.store_name
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE s.sqft BETWEEN 12007 AND 14568
