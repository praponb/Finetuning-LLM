WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    segment,
    CASE WHEN age >= 66 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    PERCENTILE_APPROX(age, 0.95) AS age_p95,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    GROUPING_ID() AS grouping_level
FROM latest
GROUP BY GROUPING SETS ((segment, CASE WHEN age >= 66 THEN 'HIGH' ELSE 'LOW' END), (segment))
HAVING age_p95 > 0
ORDER BY age_tier NULLS LAST
