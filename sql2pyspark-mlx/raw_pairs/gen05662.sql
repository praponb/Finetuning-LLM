SELECT
    status,
    COUNT(DISTINCT monthly_fee) AS unique_statuss,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count,
    AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
GROUP BY status
HAVING avg_monthly_fee >= 130;
