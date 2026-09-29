WITH Src AS (
    SELECT *
    FROM customers
    WHERE segment = 'SMB' AND email LIKE '%@yahoo.com' AND segment = 'Consumer'
),
city_summary AS (
    SELECT city, MIN(signup_date) AS first_signup_date, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
    FROM Src
    GROUP BY city
)
SELECT city, first_signup_date, credit_limit_range
FROM city_summary
WHERE credit_limit_range >= 43
ORDER BY credit_limit_range DESC
