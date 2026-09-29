WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS row_num
    FROM store_sales
)
SELECT *
FROM numbered
WHERE row_num <= 2
