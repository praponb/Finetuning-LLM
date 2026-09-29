WITH TopInventory AS (
    SELECT
        id,
        product_id,
        quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rn
    FROM inventory
)
SELECT id, product_id, quantity
FROM TopInventory
WHERE rn <= 2
ORDER BY id DESC;
