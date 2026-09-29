WITH ranked_store_sales AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS pos
    FROM store_sales
)
SELECT
    id,
    product_id,
    sale_date,
    revenue
FROM ranked_store_sales
WHERE pos <= 2
ORDER BY id, sale_date
