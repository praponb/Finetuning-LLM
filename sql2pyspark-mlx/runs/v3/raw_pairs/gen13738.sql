WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS row_num
    FROM store_sales
)
SELECT
    id,
    store_id,
    revenue,
    sale_date
FROM ranked_store_sales
WHERE row_num = 1
ORDER BY store_id, sale_date
