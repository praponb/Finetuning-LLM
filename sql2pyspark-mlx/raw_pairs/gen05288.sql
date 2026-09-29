WITH filtered_customers AS (
    SELECT *, ROUND(age * 1.1, 2) AS adjusted_age
    FROM customers
    WHERE country = 'UK'
),
customers_stats AS (
    SELECT QUARTER(signup_date) AS signup_quarter, ROUND(STDDEV(age), 2) AS stddev_age, COUNT(age) AS age_count
    FROM filtered_customers
    GROUP BY QUARTER(signup_date)
)
SELECT signup_quarter, stddev_age
FROM customers_stats
ORDER BY stddev_age DESC;
