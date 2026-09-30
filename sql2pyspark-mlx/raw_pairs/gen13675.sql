SELECT
    id,
    status,
    amount,
    paid_at
FROM (
    SELECT
        id,
        status,
        amount,
        paid_at,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS pos
    FROM payments
) AS r
WHERE pos = 1
ORDER BY paid_at;
