WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE amount BETWEEN 610 AND 920
),
PaymentsStats AS (
    SELECT status, MAX(paid_at) last_paid_at, AVG(amount) avg_amount
    FROM recent_payments
    GROUP BY status
)
SELECT *
FROM PaymentsStats
