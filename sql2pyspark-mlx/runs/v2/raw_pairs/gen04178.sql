WITH base AS (
    SELECT *
    FROM store_sales
    WHERE store_id IN (2, 5)
),
store_sales_stats AS (
    SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, product_id, COUNT(*) AS store_sale_count
    FROM base
    GROUP BY DATE_TRUNC('MONTH', sale_date), product_id
)
SELECT *
FROM store_sales_stats
ORDER BY product_id DESC, sale_month
LIMIT 50
