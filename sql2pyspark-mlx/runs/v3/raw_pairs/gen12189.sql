SELECT *
FROM (
    SELECT status, SUM(monthly_fee) AS total_monthly_fee, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    WHERE monthly_fee BETWEEN 70 AND 100
    GROUP BY status
) AS s
WHERE monthly_fee_range > 79
ORDER BY total_monthly_fee DESC
