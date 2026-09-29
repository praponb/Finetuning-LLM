SELECT status, avg_monthly_fee, monthly_fee_range
FROM (
    SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY status
) AS sub
WHERE monthly_fee_range > 61
ORDER BY monthly_fee_range DESC
