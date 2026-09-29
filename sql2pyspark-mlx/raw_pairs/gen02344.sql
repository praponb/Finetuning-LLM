SELECT QUARTER(signup_date) AS signup_quarter, MIN(signup_date) AS min_signup_date
FROM customers
WHERE credit_limit >= 15500
GROUP BY QUARTER(signup_date);
