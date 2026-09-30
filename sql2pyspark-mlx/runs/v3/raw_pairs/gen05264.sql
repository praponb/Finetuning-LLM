SELECT s.id, s.city
FROM stores s
WHERE s.id IN (SELECT store_id FROM inventory WHERE last_restock < '2025-05-01');
