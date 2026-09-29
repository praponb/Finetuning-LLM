WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE status = 'REFUNDED' AND HOUR(paid_at) BETWEEN 6 AND 13
),
summary AS (
    SELECT CASE WHEN amount >= 1590 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END AS amount_bucket, MAX(paid_at) AS last_paid_at
    FROM recent_payments
    GROUP BY CASE WHEN amount >= 1590 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END
)
SELECT amount_bucket, last_paid_at
FROM summary
