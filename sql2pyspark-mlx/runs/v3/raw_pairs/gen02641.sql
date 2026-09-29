WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE country = 'JP'
),
agg AS (
    SELECT MONTH(signup_date) signup_month, ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_corporate
    FROM filtered_customers
    GROUP BY MONTH(signup_date)
)
SELECT *
FROM agg
ORDER BY pct_corporate ASC, signup_month ASC;
