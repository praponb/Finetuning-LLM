WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age, id) AS rank_in_group
        FROM customers
    ) x
    WHERE rank_in_group = 1
)
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    COUNT_IF(country = 'JP') AS jp_events,
    SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) AS corporate_age,
    GROUPING(DATE_TRUNC('MONTH', signup_date)) AS grouping_id
FROM latest
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', signup_date), CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('MONTH', signup_date)), ())
ORDER BY age_tier DESC;
