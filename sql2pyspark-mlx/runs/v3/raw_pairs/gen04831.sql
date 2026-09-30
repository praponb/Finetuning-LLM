WITH base AS (
    SELECT *
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 6 AND 11 AND order_id IS NOT NULL AND order_id = 35
),
status_summary AS (
    SELECT status, COUNT(*) payment_count
    FROM base
    GROUP BY status
)
SELECT status, payment_count
FROM status_summary
ORDER BY status, payment_count DESC;
