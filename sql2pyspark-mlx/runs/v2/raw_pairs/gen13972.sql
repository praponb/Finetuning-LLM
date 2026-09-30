WITH top_inventory AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
    FROM inventory
    WHERE product_id IN (1, 4, 5)
)
SELECT *
FROM top_inventory
WHERE rn = 1;
