SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(age) AS age_count, MIN(credit_limit) AS min_credit_limit FROM customers GROUP BY DATE_TRUNC('MONTH', signup_date);
