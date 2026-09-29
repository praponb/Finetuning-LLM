SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN age >= 73 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    city,
    MIN(age) AS min_age,
    STDDEV_SAMP(age) AS age_stddev,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    GROUPING(DATE_TRUNC('QUARTER', signup_date)) AS grouping_level
FROM customers
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), CASE WHEN age >= 73 THEN 'HIGH' ELSE 'LOW' END, city), (DATE_TRUNC('QUARTER', signup_date), CASE WHEN age >= 73 THEN 'HIGH' ELSE 'LOW' END))
HAVING min_age >= 0
ORDER BY age_stddev DESC
