WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE country = 'JP'
),
agg AS (
    SELECT MONTH(signup_date) signup_month, SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END) corporate_count
    FROM filtered_customers
    GROUP BY MONTH(signup_date)
)
SELECT *
FROM agg
WHERE corporate_count >= 74
ORDER BY corporate_count ASC;
