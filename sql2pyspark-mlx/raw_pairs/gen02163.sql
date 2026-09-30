SELECT segment, DATE_TRUNC('MONTH', signup_date) signup_month, MIN(credit_limit) min_credit_limit
FROM customers
GROUP BY segment, signup_month
ORDER BY signup_month, segment
LIMIT 5;
