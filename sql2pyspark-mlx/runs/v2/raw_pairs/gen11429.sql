SELECT
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    COUNT(DISTINCT credit_limit) AS unique_segments,
    AVG(credit_limit) AS avg_credit_limit,
    SUM(CASE WHEN country = 'JP' THEN age ELSE 0 END) AS jp_age
FROM customers
GROUP BY signup_month
HAVING SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END) > 30
ORDER BY avg_credit_limit DESC, jp_age;
