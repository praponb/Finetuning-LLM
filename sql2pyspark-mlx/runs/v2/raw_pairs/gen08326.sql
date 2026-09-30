SELECT method, MONTH(paid_at) AS paid_month, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
GROUP BY method, MONTH(paid_at)
HAVING COUNT(CASE WHEN status = 'FAILED' THEN 1 END) < 4
ORDER BY method ASC, failed_count DESC;
