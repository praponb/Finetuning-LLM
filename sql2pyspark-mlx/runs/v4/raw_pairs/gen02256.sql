SELECT
    id,
    status,
    method,
    amount,
    NTILE(2) OVER (PARTITION BY status, method ORDER BY amount, id) AS amount_ntile
FROM payments
ORDER BY amount DESC
