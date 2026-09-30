SELECT
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    SUM(SUM(credit_limit)) OVER (ORDER BY DATE_TRUNC('MONTH', signup_date)) AS cumulative_credit_limit
FROM customers
GROUP BY DATE_TRUNC('MONTH', signup_date)
ORDER BY signup_month
