WITH ranked_inventory AS (
    SELECT
        id,
        store_id,
        quantity,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS pos
    FROM inventory
)
SELECT *
FROM ranked_inventory
WHERE pos <= 3
