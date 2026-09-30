SELECT MIN(signup_date) AS min_signup_date, ROUND(STDDEV(age), 2) AS stddev_age FROM customers;
