WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE method IN ('cash', 'bank_transfer') AND status IN ('SUCCESS', 'FAILED', 'REFUNDED')
),
summary AS (
    SELECT method, ROUND(AVG(amount), 2) AS avg_amount
    FROM filtered_payments
    GROUP BY method
)
SELECT method, avg_amount
FROM summary
ORDER BY avg_amount DESC
