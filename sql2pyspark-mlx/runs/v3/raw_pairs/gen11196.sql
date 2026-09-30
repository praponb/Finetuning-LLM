SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE status IN ('ACTIVE', 'PAUSED') AND plan = 'standard' AND end_date BETWEEN '2023-10-01' AND '2025-02-17'
GROUP BY status
ORDER BY avg_monthly_fee DESC;
