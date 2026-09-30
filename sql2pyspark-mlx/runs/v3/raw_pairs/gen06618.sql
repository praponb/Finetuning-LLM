WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
    FROM inventory
    WHERE store_id IN (2, 3, 4, 5)
)
SELECT *
FROM numbered
WHERE rn <= 1;
