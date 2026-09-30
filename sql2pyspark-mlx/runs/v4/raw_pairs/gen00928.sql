WITH ranked AS (
    SELECT
        id,
        method,
        paid_at,
        amount,
        RANK() OVER (PARTITION BY method ORDER BY paid_at) AS rn
    FROM payments
    WHERE amount <= 990
)
SELECT
    id,
    method,
    paid_at,
    amount
FROM ranked
WHERE rn <= 3
ORDER BY amount DESC
