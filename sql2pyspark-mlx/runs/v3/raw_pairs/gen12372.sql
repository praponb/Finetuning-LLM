SELECT
    id,
    status,
    method,
    DATE_TRUNC('week', paid_at) AS paid_at_period
FROM payments
WHERE status NOT IN ('SUCCESS', 'REFUNDED')
ORDER BY method DESC;
