WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) AS pos
    FROM payments
)
SELECT
    id,
    method,
    paid_at,
    amount
FROM numbered
WHERE pos <= 5;
