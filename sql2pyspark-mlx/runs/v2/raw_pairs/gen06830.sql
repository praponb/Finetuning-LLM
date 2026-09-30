WITH top_inventory AS (
    SELECT
        id,
        product_id,
        quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS pos
    FROM inventory
)
SELECT *
FROM top_inventory
WHERE pos <= 2
ORDER BY quantity;
