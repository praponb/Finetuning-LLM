SELECT
    id,
    status,
    method,
    amount,
    RANK() OVER (PARTITION BY status, method ORDER BY amount DESC) AS rnk
FROM payments;
