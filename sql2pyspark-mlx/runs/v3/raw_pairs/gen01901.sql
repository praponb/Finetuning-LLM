SELECT MONTH(signup_date) AS signup_month, COUNT(*) AS row_count, MAX(signup_date) AS latest_signup_date FROM customers GROUP BY 1;
