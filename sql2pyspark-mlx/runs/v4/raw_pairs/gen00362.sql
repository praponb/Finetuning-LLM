SELECT is_active, COUNT(credit_limit) AS credit_limit_count, MIN(signup_date) AS first_signup_date
FROM customers
WHERE email IS NOT NULL
GROUP BY is_active;
