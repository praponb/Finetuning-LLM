SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, MAX(credit_limit) AS max_credit_limit FROM customers GROUP BY DATE_TRUNC('MONTH', signup_date);
