SELECT end_month, avg_monthly_fee, subscription_count
FROM (
    SELECT DATE_FORMAT(end_date, 'yyyy-MM') end_month, AVG(monthly_fee) avg_monthly_fee, COUNT(*) subscription_count
    FROM subscriptions
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
) s
WHERE subscription_count < 7
ORDER BY avg_monthly_fee DESC
