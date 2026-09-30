WITH ranked_store_sales AS (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS row_num
    FROM store_sales
)
SELECT *
FROM ranked_store_sales
WHERE row_num <= 3
ORDER BY row_num DESC
