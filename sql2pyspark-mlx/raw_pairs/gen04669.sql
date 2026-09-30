WITH recent_stores AS (
    SELECT *
    FROM stores
    WHERE sqft < 15736 AND sqft <> 15109
),
summary AS (
    SELECT
        MONTH(opened_date) opened_month,
        region,
        ROUND((100 * SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_central,
        COUNT(sqft) sqft_count
    FROM recent_stores
    GROUP BY MONTH(opened_date), region
)
SELECT opened_month, region, sqft_count
FROM summary
WHERE sqft_count > 9
ORDER BY sqft_count;
