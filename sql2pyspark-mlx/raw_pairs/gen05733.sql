SELECT
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    MAX(age) - MIN(age) AS age_range,
    MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range,
    SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) AS jp_count
FROM customers
WHERE country NOT IN ('US', 'UK') OR age <= 34
GROUP BY signup_month
ORDER BY age_range DESC;
