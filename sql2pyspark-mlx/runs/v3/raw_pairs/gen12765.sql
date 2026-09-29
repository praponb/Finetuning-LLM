SELECT YEAR(signup_date) AS signup_year, COUNT(*) AS customer_count, MAX(signup_date) AS last_signup_date
FROM customers
WHERE credit_limit > 6700
GROUP BY YEAR(signup_date);
