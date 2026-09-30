WITH base AS (
    SELECT *
    FROM stores
    WHERE region IN ('East', 'West', 'Central')
),
summary AS (
    SELECT CASE WHEN sqft >= 15834 THEN 'high' WHEN sqft >= 6124 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(opened_date) AS first_opened_date, AVG(sqft) AS avg_sqft
    FROM base
    GROUP BY CASE WHEN sqft >= 15834 THEN 'high' WHEN sqft >= 6124 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY avg_sqft DESC;
