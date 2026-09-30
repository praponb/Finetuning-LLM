WITH RankedInventory AS (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS pos
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    quantity
FROM RankedInventory
WHERE pos <= 3;
