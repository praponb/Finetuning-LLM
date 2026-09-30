WITH ranked AS (
    SELECT
        id,
        order_id,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS rnk
    FROM payments
    WHERE status IN ('REFUNDED', 'FAILED')
)
SELECT id, order_id, paid_at
FROM ranked
WHERE rnk <= 1;
