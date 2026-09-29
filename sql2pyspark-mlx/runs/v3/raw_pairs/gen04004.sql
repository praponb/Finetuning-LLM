SELECT status, SUM(monthly_fee) AS total_monthly_fee, SUM(SUM(monthly_fee)) OVER (ORDER BY status) AS cumulative_monthly_fee
FROM subscriptions
GROUP BY status
ORDER BY status;
