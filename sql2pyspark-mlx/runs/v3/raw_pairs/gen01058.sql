SELECT s.id, s.city, s.opened_date
FROM stores s
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id IN (5, 10))
ORDER BY s.opened_date DESC;
