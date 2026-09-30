SELECT
    id,
    status,
    amount,
    paid_at
FROM (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount DESC) row_num
    FROM payments
    WHERE order_id IS NOT NULL
) r
WHERE row_num <= 1;
