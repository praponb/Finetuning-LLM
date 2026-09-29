WITH FilteredCustomers AS (
    SELECT *
    FROM customers
    WHERE city = 'Berlin' AND name IS NOT NULL
),
city_summary AS (
    SELECT city, is_active, MAX(signup_date) AS latest_signup_date
    FROM FilteredCustomers
    GROUP BY city, is_active
)
SELECT *
FROM city_summary
ORDER BY 1 DESC
