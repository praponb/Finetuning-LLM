WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) pos
    FROM inventory
    WHERE last_restock < '2025-03-01'
)
SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM ranked
WHERE pos <= 1
ORDER BY id ASC;
