SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, city, COUNT(credit_limit) AS credit_limit_count FROM customers GROUP BY signup_month, city HAVING COUNT(credit_limit) >= 2;
