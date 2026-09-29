WITH Ranked AS (
    SELECT
        id,
        status,
        amount,
        paid_at,
        RANK() OVER (PARTITION BY status ORDER BY amount) AS pos
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS')
)
SELECT
    id,
    status,
    amount,
    paid_at
FROM Ranked
WHERE pos <= 5;
