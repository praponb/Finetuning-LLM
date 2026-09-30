SELECT id, store_id, product_id
FROM inventory
WHERE quantity >= 101
UNION ALL
SELECT id, store_id, product_id
FROM inventory
WHERE last_restock BETWEEN '2023-08-13' AND '2024-07-01'
