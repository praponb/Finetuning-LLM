WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS row_num
    FROM store_sales
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY revenue DESC, store_id
