WITH ranked_payments AS (
    SELECT
        id,
        method,
        amount,
        order_id,
        RANK() OVER (PARTITION BY method ORDER BY amount) AS row_num
    FROM payments
    WHERE amount > 510
)
SELECT
    id,
    method,
    amount,
    order_id
FROM ranked_payments
WHERE row_num <= 2
