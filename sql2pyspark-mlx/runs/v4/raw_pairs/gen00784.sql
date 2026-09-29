WITH recent_products AS (
    SELECT *, INITCAP(TRIM(product_name)) product_name_title
    FROM products
    WHERE MONTH(launch_date) IN (1, 5, 11)
),
launch_quarter_summary AS (
    SELECT QUARTER(launch_date) launch_quarter, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) sports_count
    FROM recent_products
    GROUP BY QUARTER(launch_date)
)
SELECT *
FROM launch_quarter_summary
ORDER BY 2 DESC, launch_quarter ASC
LIMIT 20;
