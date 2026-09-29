WITH top_payments AS (
    SELECT
        id,
        order_id,
        paid_at,
        RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS pos
    FROM payments
)
SELECT *
FROM top_payments
WHERE pos <= 3
ORDER BY 1 DESC, pos;
