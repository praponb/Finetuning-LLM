WITH FilteredPayments AS (
    SELECT *
    FROM payments
    WHERE amount IS NULL AND (HOUR(paid_at) BETWEEN 7 AND 10 OR method IN ('cash', 'promptpay'))
),
summary AS (
    SELECT
        status,
        CASE WHEN amount >= 1530 THEN 'high' WHEN amount >= 210 THEN 'mid' ELSE 'low' END AS amount_bucket,
        AVG(amount) AS avg_amount,
        SUM(amount) AS total_amount
    FROM FilteredPayments
    GROUP BY status, CASE WHEN amount >= 1530 THEN 'high' WHEN amount >= 210 THEN 'mid' ELSE 'low' END
)
SELECT status, amount_bucket, avg_amount
FROM summary
