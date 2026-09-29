WITH recent_customers AS (
    SELECT *, TRIM(city) AS clean_city
    FROM customers
    WHERE signup_date < '2024-05-08' OR signup_date < '2025-05-01'
),
agg AS (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, MIN(age) AS min_age
    FROM recent_customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
)
SELECT *
FROM agg
ORDER BY min_age DESC, signup_month;
