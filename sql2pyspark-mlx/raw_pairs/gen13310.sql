SELECT
    id,
    status,
    paid_at,
    amount
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY paid_at) AS rn
    FROM payments
) AS t
WHERE rn <= 5;
