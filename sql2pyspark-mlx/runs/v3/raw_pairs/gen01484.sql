WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
    FROM inventory
)
SELECT *
FROM Numbered
WHERE rn <= 2
