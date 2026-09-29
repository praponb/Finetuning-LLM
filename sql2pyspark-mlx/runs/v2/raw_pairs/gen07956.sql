SELECT i.id, i.store_id, s.sqft
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE s.sqft <= 11943 OR i.store_id IN (5, 6);
