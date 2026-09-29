SELECT
    method,
    status,
    DATE_TRUNC('MONTH', paid_at) AS paid_month,
    AVG(amount) AS avg_amount
FROM payments
GROUP BY method, status, DATE_TRUNC('MONTH', paid_at)
ORDER BY method DESC;
