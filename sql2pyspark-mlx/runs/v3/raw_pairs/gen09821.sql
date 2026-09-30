SELECT MONTH(signup_date) signup_month, MAX(signup_date) last_signup_date, MAX(signup_date) latest_signup_date FROM customers GROUP BY signup_month HAVING COUNT(age) < 5;
