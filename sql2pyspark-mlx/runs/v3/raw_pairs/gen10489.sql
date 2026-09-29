WITH ranked AS (
    SELECT
        id,
        order_id,
        amount,
        status,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount) AS pos
    FROM payments
)
SELECT
    id,
    order_id,
    amount,
    status
FROM ranked
WHERE pos <= 1
ORDER BY amount DESC, order_id DESC
