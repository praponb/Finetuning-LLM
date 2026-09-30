WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC, id) AS rn
    FROM payments
    QUALIFY rn = 1
)
SELECT
    id,
    amount,
    paid_at
FROM latest
ORDER BY paid_at DESC
