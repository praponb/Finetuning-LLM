SELECT
    is_active,
    CASE WHEN credit_limit >= 13400 THEN 'high' WHEN credit_limit >= 4000 THEN 'mid' ELSE 'low' END credit_limit_bucket,
    ROUND(AVG(credit_limit), 2) avg_credit_limit,
    MAX(signup_date) latest_signup_date,
    SUM(credit_limit) total_credit_limit
FROM customers
WHERE (is_active AND city IN ('Chiang Mai', 'Berlin', 'Singapore')) OR segment IS NULL
GROUP BY is_active, credit_limit_bucket;
