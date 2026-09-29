WITH src AS (
    SELECT *
    FROM customers
    WHERE credit_limit <= 8700 AND LENGTH(email) > 12
),
summary AS (
    SELECT segment, SUM(credit_limit) AS total_credit_limit
    FROM src
    GROUP BY segment
)
SELECT *
FROM summary
WHERE total_credit_limit > 31000
ORDER BY total_credit_limit;
