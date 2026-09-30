WITH Numbered AS (
    SELECT
        id,
        product_id,
        last_restock,
        RANK() OVER (PARTITION BY product_id ORDER BY last_restock) AS pos
    FROM inventory
)
SELECT *
FROM Numbered
WHERE pos = 1
