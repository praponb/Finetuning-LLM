WITH valid_products AS (
    SELECT *
    FROM products
    WHERE product_name LIKE 'W%' AND product_name LIKE 'Sma%'
),
launch_month_summary AS (
    SELECT
        DATE_TRUNC('MONTH', launch_date) AS launch_month,
        CASE WHEN brand IN ('Umbrella', 'Acme') THEN 'priority' ELSE 'other' END AS brand_group,
        COUNT(DISTINCT stock_qty) AS unique_brands,
        ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM valid_products
    GROUP BY DATE_TRUNC('MONTH', launch_date), CASE WHEN brand IN ('Umbrella', 'Acme') THEN 'priority' ELSE 'other' END
)
SELECT *
FROM launch_month_summary
ORDER BY unique_brands DESC, launch_month ASC, brand_group ASC
LIMIT 5;
