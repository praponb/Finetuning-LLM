WITH ranked_payments AS (
    SELECT
        id,
        method,
        amount,
        status,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS pos
    FROM payments
)
SELECT
    id,
    method,
    amount,
    status
FROM ranked_payments
WHERE pos <= 1
