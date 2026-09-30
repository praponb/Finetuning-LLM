WITH src AS (
    SELECT *, age + credit_limit AS age_plus_credit_limit
    FROM customers
    WHERE age = 63 AND name LIKE 'Ali%'
),
customers_stats AS (
    SELECT segment, ROUND(AVG(age_plus_credit_limit), 2) AS avg_age_plus_credit_limit, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_de
    FROM src
    GROUP BY segment
)
SELECT *
FROM customers_stats;
