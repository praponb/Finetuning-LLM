SELECT s.id, s.opened_date
FROM stores s
WHERE s.store_name LIKE '%Old%' AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id IN (3, 5, 7));
