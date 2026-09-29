WITH ranked_inventory AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS pos
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM ranked_inventory
WHERE pos <= 2
ORDER BY last_restock;
