SELECT s.id, s.store_name
FROM stores s
WHERE s.region IN ('East', 'South') AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id);
