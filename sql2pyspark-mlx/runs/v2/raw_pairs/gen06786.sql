SELECT city, DATE_TRUNC('MONTH', signup_date) AS signup_month, MAX(credit_limit) AS max_credit_limit FROM customers GROUP BY 1, 2 ORDER BY city;
