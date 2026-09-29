WITH top_store_sales AS (
    SELECT
        id,
        store_id,
        revenue,
        sale_date,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS row_num
    FROM store_sales
)
SELECT
    id,
    store_id,
    revenue,
    sale_date
FROM top_store_sales
WHERE row_num <= 1;
