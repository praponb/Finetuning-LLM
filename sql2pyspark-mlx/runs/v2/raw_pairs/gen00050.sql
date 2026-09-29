SELECT s.id, s.opened_date
FROM stores s
WHERE s.region = 'East' AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.store_id IN (1, 2, 5, 6))
ORDER BY id DESC, s.opened_date;
