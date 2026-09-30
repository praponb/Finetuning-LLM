WITH src AS (
    SELECT *
    FROM store_sales
    WHERE revenue < 1100 AND revenue > 3800
),
store_sales_stats AS (
    SELECT
        product_id,
        store_id,
        AVG(units) AS avg_units,
        MAX(revenue) - MIN(revenue) AS revenue_range
    FROM src
    GROUP BY product_id, store_id
)
SELECT *
FROM store_sales_stats
ORDER BY product_id, store_id DESC
LIMIT 10;
