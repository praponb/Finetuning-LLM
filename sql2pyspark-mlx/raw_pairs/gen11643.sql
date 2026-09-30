SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, city, MAX(credit_limit) AS max_credit_limit, COUNT(*) AS num_customers FROM customers GROUP BY DATE_TRUNC('MONTH', signup_date), city;
