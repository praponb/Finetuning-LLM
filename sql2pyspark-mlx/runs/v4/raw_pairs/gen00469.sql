WITH src AS (
    SELECT *
    FROM customers
    WHERE is_active
),
agg AS (
    SELECT country, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
    FROM src
    GROUP BY country
)
SELECT country, avg_credit_limit
FROM agg
WHERE avg_credit_limit < 64
ORDER BY country
