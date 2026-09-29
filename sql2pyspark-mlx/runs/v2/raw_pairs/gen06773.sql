WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS rn
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    quantity
FROM Ranked
WHERE rn <= 5
ORDER BY 1 DESC
