SELECT
    s.id,
    s.sqft,
    s.opened_date,
    s.city
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id)
ORDER BY id DESC, s.city;
