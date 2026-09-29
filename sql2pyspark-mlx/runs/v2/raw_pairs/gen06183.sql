SELECT segment, DATE_TRUNC('MONTH', signup_date) AS signup_month, MIN(signup_date) AS first_signup_date FROM customers GROUP BY segment, DATE_TRUNC('MONTH', signup_date) ORDER BY segment;
