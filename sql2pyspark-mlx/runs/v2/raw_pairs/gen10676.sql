WITH recent_stores AS (
    SELECT *, ADD_MONTHS(opened_date, 3) AS opened_date_plus_months
    FROM stores
    WHERE MONTH(opened_date) IN (5, 6, 8) AND LENGTH(store_name) >= 11
),
opened_plus_months_month_summary AS (
    SELECT
        DATE_FORMAT(opened_date_plus_months, 'yyyy-MM') AS opened_plus_months_month,
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count,
        SUM(sqft) AS total_sqft
    FROM recent_stores
    GROUP BY DATE_FORMAT(opened_date_plus_months, 'yyyy-MM'), DATE_TRUNC('MONTH', opened_date)
)
SELECT *
FROM opened_plus_months_month_summary
WHERE total_sqft >= 79000
ORDER BY south_count, opened_month
