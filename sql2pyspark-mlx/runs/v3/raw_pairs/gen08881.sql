SELECT
    id,
    status,
    LOWER(method) AS method_lower,
    ROUND(amount, 2) AS amount_rounded
FROM payments
WHERE NOT (HOUR(paid_at) BETWEEN 12 AND 18 AND status IN ('SUCCESS', 'FAILED') AND paid_at < '2025-01-01')
