WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE email IS NOT NULL OR email LIKE '%@startup.io'
),
agg AS (
    SELECT YEAR(signup_date) AS signup_year, MIN(age) AS min_age
    FROM recent_customers
    GROUP BY YEAR(signup_date)
)
SELECT *
FROM agg
WHERE min_age < 58
ORDER BY min_age DESC, signup_year DESC;
