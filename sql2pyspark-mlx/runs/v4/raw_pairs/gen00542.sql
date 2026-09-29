SELECT ss.id, ss.store_id
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE city IN ('Bangkok', 'London'));
