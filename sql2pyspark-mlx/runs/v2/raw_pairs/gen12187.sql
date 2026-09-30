WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE amount BETWEEN 920 AND 930 AND paid_at < '2023-09-14'
),
status_summary AS (
    SELECT status, MAX(amount) - MIN(amount) AS amount_range
    FROM recent_payments
    GROUP BY status
)
SELECT status, amount_range
FROM status_summary;
