WITH src AS (
    SELECT *
    FROM stores
    WHERE opened_date <= '2025-08-01' OR UPPER(TRIM(store_name)) = 'SIAM FLAGSHIP'
),
region_summary AS (
    SELECT region, MONTH(opened_date) AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM src
    GROUP BY region, MONTH(opened_date)
)
SELECT *
FROM region_summary
ORDER BY stddev_sqft DESC
