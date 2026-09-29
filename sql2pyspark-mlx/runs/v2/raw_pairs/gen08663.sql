SELECT
    DATE_TRUNC('MONTH', signup_date) signup_month,
    COUNT(age) age_count,
    SUM(credit_limit) total_credit_limit,
    MIN(signup_date) min_signup_date
FROM customers
GROUP BY DATE_TRUNC('MONTH', signup_date)
HAVING MAX(credit_limit) < 5200
ORDER BY age_count, signup_month DESC
