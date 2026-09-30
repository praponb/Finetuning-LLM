WITH valid_products AS (
    SELECT *, LENGTH(brand) AS brand_length
    FROM products
    WHERE MONTH(launch_date) IN (1, 11, 12) AND stock_qty BETWEEN 219 AND 332 AND price >= 1120
),
summary AS (
    SELECT MONTH(launch_date) AS launch_month, COUNT(DISTINCT price) AS unique_brands, MAX(launch_date) AS last_launch_date
    FROM valid_products
    GROUP BY MONTH(launch_date)
)
SELECT launch_month, last_launch_date
FROM summary
WHERE unique_brands >= 7
ORDER BY last_launch_date, launch_month DESC
