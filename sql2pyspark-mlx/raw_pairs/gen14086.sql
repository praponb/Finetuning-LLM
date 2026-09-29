WITH src AS (
    SELECT *
    FROM payments
    WHERE TO_DATE(paid_at) >= '2024-05-01'
),
agg AS (
    SELECT status, ROUND(AVG(amount), 2) AS avg_amount
    FROM src
    GROUP BY status
)
SELECT status, avg_amount
FROM agg
WHERE avg_amount >= 20
ORDER BY status, avg_amount DESC
