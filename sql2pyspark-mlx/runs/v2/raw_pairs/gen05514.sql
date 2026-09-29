SELECT
    s.id,
    s.region,
    s.opened_date,
    s.city
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id);
