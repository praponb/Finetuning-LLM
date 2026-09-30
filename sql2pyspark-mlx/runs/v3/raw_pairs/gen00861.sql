WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) AS row_num
    FROM orders
)
SELECT *
FROM numbered
WHERE row_num <= 2
ORDER BY store_id
