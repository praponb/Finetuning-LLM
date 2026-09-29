SELECT id, last_restock, product_id
FROM inventory
WHERE last_restock <= '2023-06-24'
UNION ALL
SELECT id, last_restock, product_id
FROM inventory
WHERE store_id = 3;
