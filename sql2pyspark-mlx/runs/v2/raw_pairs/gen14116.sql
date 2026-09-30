SELECT age, signup_date, is_active, DATE_TRUNC('week', signup_date) AS signup_date_period FROM customers
