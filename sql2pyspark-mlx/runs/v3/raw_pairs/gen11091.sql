WITH ranked_inventory AS (
    SELECT
        id,
        store_id,
        quantity,
        product_id,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS row_num
    FROM inventory
    WHERE last_restock BETWEEN '2023-03-01' AND '2024-03-01'
)
SELECT *
FROM ranked_inventory
WHERE row_num <= 3
ORDER BY id DESC
