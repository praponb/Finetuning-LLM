SELECT DATE_TRUNC('MONTH', signup_date) signup_month, MAX(signup_date) last_signup_date, ROUND(STDDEV(credit_limit), 2) stddev_credit_limit
FROM customers
GROUP BY signup_month
ORDER BY signup_month DESC
