WITH valid_customers AS (
    SELECT *, LOWER(country) country_lower
    FROM customers
    WHERE name LIKE '%Chaiya%'
),
age_bucket_summary AS (
    SELECT
        CASE WHEN age >= 71 THEN 'high' WHEN age >= 29 THEN 'mid' ELSE 'low' END age_bucket,
        city,
        MAX(age) - MIN(age) age_range,
        COUNT(*) total_count
    FROM valid_customers
    GROUP BY CASE WHEN age >= 71 THEN 'high' WHEN age >= 29 THEN 'mid' ELSE 'low' END, city
)
SELECT *
FROM age_bucket_summary
ORDER BY total_count DESC
