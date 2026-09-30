SELECT
    id,
    status,
    amount,
    paid_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS rnk
    FROM payments
    WHERE status = 'REFUNDED'
) AS ranked
WHERE rnk <= 5
