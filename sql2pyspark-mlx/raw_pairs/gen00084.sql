WITH TopInventory AS (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    quantity
FROM TopInventory
WHERE rnk = 1;
