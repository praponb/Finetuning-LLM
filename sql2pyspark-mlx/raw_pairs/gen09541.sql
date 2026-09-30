WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS row_num
    FROM store_sales
)
SELECT
    id,
    product_id,
    sale_date,
    store_id
FROM ranked_store_sales
WHERE row_num = 1
ORDER BY id DESC
