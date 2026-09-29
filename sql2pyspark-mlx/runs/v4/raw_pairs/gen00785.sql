SELECT s.id, s.region, s.opened_date FROM stores s WHERE s.id IN (SELECT store_id FROM inventory WHERE store_id IN (1, 3, 5, 6)) ORDER BY s.id
