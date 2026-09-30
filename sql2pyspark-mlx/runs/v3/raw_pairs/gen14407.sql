SELECT is_active, MIN(age) AS min_age, MIN(signup_date) AS first_signup_date
FROM customers
GROUP BY is_active
ORDER BY first_signup_date, min_age
