SELECT *
FROM orders
WHERE store_id IN (19, 22, 26, 33)
ORDER BY status DESC, id
LIMIT 20;
