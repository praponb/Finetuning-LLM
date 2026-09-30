SELECT
    s.id,
    s.sqft,
    s.store_name,
    s.region
FROM stores s
WHERE s.city = 'Berlin' AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id)
