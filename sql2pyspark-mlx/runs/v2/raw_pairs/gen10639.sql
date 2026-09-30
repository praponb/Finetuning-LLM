SELECT
    id,
    status,
    paid_at,
    SUBSTRING(method, 1, 2) AS method_prefix
FROM payments
WHERE NOT (status = 'REFUNDED' AND HOUR(paid_at) BETWEEN 7 AND 11 AND status IN ('SUCCESS', 'FAILED', 'REFUNDED'))
ORDER BY id DESC
