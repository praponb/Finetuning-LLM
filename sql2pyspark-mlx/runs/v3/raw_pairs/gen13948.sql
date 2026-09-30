SELECT s.id, s.store_name
FROM stores s
WHERE s.store_name LIKE '%Old%' AND s.id IN (SELECT store_id FROM store_sales);
