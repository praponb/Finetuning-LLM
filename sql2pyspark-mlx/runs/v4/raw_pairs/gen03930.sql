WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id) AS row_num
    FROM store_sales
    QUALIFY row_num = 1
)
SELECT
    id,
    sale_date,
    revenue,
    row_num
FROM ranked
