SELECT
    MONTH(signup_date) AS signup_month,
    MAX(signup_date) AS last_signup_date,
    MIN(age) AS min_age,
    AVG(age) AS avg_age
FROM customers
GROUP BY 1
ORDER BY min_age;
