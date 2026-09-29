SELECT s.store_name, i.store_id, i.quantity
FROM stores s
LEFT OUTER JOIN inventory i ON i.store_id = s.id
WHERE s.sqft <= 5036
