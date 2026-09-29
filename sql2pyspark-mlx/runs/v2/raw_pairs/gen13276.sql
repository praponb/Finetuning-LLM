SELECT signup_year, first_signup_date, avg_age
FROM (
    SELECT YEAR(signup_date) AS signup_year, MIN(signup_date) AS first_signup_date, AVG(age) AS avg_age
    FROM customers
    GROUP BY YEAR(signup_date)
) AS t
WHERE avg_age >= 53
