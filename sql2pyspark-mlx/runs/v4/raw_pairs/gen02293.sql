SELECT
    id,
    method,
    amount,
    paid_at
FROM (
    SELECT
        id,
        method,
        amount,
        paid_at,
        RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS rn
    FROM payments
) AS ranked
WHERE rn <= 3
