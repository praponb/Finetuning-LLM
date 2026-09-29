WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS pos
    FROM inventory
    WHERE product_id IN (3, 6)
)
SELECT *
FROM numbered
WHERE pos <= 1
ORDER BY product_id DESC
