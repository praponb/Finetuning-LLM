SELECT
    id,
    status,
    amount,
    method,
    SUBSTRING(status, 1, 3) AS status_prefix,
    HOUR(paid_at) AS paid_at_hour
FROM payments
WHERE status = 'SUCCESS' OR status <> 'FAILED';
