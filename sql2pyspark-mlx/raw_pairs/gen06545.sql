WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE region IN ('South', 'North')
),
Summary AS (
    SELECT
        city,
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        AVG(sqft) AS avg_sqft,
        MAX(sqft) - MIN(sqft) AS sqft_range
    FROM valid_stores
    GROUP BY city, DATE_TRUNC('MONTH', opened_date)
)
SELECT
    city,
    opened_month,
    avg_sqft,
    sqft_range
FROM Summary
ORDER BY 3 DESC;
