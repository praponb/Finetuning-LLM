SELECT id, last_restock, store_id
FROM inventory
WHERE quantity >= 124
UNION
SELECT id, last_restock, store_id
FROM inventory
WHERE last_restock < '2023-10-01'
ORDER BY store_id DESC;
