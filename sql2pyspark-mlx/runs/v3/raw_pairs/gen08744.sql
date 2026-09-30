SELECT status, COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count, AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
GROUP BY status
HAVING SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) >= 35
ORDER BY expired_count DESC
