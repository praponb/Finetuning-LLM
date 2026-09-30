WITH base AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (2, 4, 6, 9)
),
store_sales_stats AS (
    SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM base
    GROUP BY product_id
)
SELECT *
FROM store_sales_stats
WHERE stddev_units >= 80
ORDER BY product_id DESC;
