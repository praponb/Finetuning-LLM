WITH filtered_customers AS (
    SELECT *, COALESCE(name, 'Unknown') name_filled
    FROM customers
    WHERE credit_limit < 14800
),
summary AS (
    SELECT YEAR(signup_date) signup_year, SUM(credit_limit) total_credit_limit, MAX(signup_date) latest_signup_date
    FROM filtered_customers
    GROUP BY YEAR(signup_date)
)
SELECT signup_year, latest_signup_date
FROM summary
WHERE total_credit_limit < 55000
