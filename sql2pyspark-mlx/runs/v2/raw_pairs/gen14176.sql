WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS row_num
    FROM store_sales
)
SELECT
    id,
    product_id,
    revenue,
    sale_date
FROM ranked
WHERE row_num <= 2
ORDER BY product_id DESC;
