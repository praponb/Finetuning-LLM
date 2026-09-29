SELECT
    id,
    quantity,
    last_restock,
    product_id
FROM inventory
WHERE last_restock < '2025-02-01' AND MONTH(last_restock) IN (3, 9, 11)
ORDER BY product_id DESC, id
LIMIT 50;
