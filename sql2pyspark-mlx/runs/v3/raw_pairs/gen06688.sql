WITH ranked_payments AS (
    SELECT
        id,
        method,
        paid_at,
        RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) rnk
    FROM payments
    WHERE order_id IS NOT NULL
)
SELECT id, method, paid_at
FROM ranked_payments
WHERE rnk <= 5
ORDER BY paid_at, method DESC
