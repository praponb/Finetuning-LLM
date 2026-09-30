WITH ranked_payments AS (
    SELECT
        id,
        order_id,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS pos
    FROM payments
    WHERE order_id IS NULL
)
SELECT id, order_id, paid_at
FROM ranked_payments
WHERE pos <= 3
