WITH src AS (
    SELECT *
    FROM customers
    WHERE LENGTH(email) < 12 AND signup_date >= '2023-08-01'
),
country_summary AS (
    SELECT country, MIN(age) AS min_age, COUNT(credit_limit) AS credit_limit_count
    FROM src
    GROUP BY country
)
SELECT country, credit_limit_count
FROM country_summary
WHERE credit_limit_count < 4
