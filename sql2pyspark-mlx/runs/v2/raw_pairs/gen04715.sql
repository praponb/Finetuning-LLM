WITH top_store_sales AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS row_num
    FROM store_sales
)
SELECT *
FROM top_store_sales
WHERE row_num <= 2
ORDER BY store_id
