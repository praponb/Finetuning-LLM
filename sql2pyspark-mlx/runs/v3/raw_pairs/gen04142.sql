WITH base AS (
    SELECT *
    FROM store_sales
    WHERE store_id IN (2, 5)
),
store_sales_stats AS (
    SELECT store_id, product_id, COUNT(*) AS store_sale_count
    FROM base
    GROUP BY store_id, product_id
)
SELECT *
FROM store_sales_stats
ORDER BY product_id DESC, store_id
LIMIT 50
