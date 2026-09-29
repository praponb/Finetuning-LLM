SELECT
    id,
    method,
    DATEDIFF(CURRENT_DATE(), paid_at) days_since_paid_at,
    SUBSTRING(status, 1, 4) status_prefix
FROM payments
WHERE status = 'SUCCESS'
ORDER BY 3 DESC
