WITH ranked AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
    WHERE YEAR(last_restock) >= 2023
)
SELECT *
FROM ranked
WHERE row_num <= 3
ORDER BY quantity
