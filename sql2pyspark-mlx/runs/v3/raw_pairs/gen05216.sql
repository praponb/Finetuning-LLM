SELECT
    city,
    MIN(age) AS min_age,
    MAX(signup_date) AS last_signup_date,
    MIN(signup_date) AS min_signup_date
FROM customers
WHERE (email LIKE '%@outlook.com' AND email LIKE '%@GMAIL.COM') OR is_active
GROUP BY city
ORDER BY min_signup_date;
