SELECT s.id, s.store_name
FROM stores s
WHERE s.id IN (SELECT store_id FROM inventory WHERE quantity != 63)
ORDER BY id DESC, store_name
LIMIT 5
