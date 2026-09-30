WITH recent_customers AS (
    SELECT *, UPPER(TRIM(email)) AS email_normalized
    FROM customers
    WHERE is_active = TRUE OR age < 44
),
customers_stats AS (
    SELECT
        MONTH(signup_date) AS signup_month,
        YEAR(signup_date) AS signup_year,
        COUNT(age) AS age_count,
        COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count
    FROM recent_customers
    GROUP BY MONTH(signup_date), YEAR(signup_date)
)
SELECT *
FROM customers_stats
WHERE age_count > 7
ORDER BY new_york_count DESC;
