SELECT id, product_id
FROM inventory
WHERE product_id = 3
UNION
SELECT id, product_id
FROM inventory
WHERE last_restock <= '2025-01-23';
