SELECT ROUND((100 * SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_us, MIN(signup_date) AS min_signup_date, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
WHERE segment IN ('Corporate', 'Consumer')
