WITH numbered AS (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS row_num
    FROM inventory
)
SELECT id, store_id, last_restock
FROM numbered
WHERE row_num <= 5
ORDER BY id DESC;
