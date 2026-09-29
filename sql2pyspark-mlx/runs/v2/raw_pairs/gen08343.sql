SELECT QUARTER(signup_date) AS signup_quarter, MIN(credit_limit) AS min_credit_limit, MAX(age) AS max_age
FROM customers
WHERE email IS NOT NULL AND credit_limit < 13200
GROUP BY signup_quarter;
