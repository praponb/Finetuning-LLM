SELECT QUARTER(signup_date) AS signup_quarter, SUM(age) AS total_age FROM customers GROUP BY QUARTER(signup_date);
