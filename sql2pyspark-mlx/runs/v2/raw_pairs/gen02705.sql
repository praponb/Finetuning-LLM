SELECT
    method,
    MONTH(paid_at) AS paid_month,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count
FROM payments
WHERE (TO_DATE(paid_at) < '2025-07-01' AND method = 'cash') OR amount <= 1310
GROUP BY method, paid_month
HAVING COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) < 2
ORDER BY success_count, method
