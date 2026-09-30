SELECT
    status,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    COUNT(*) AS num_subscriptions,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE status = 'ACTIVE'
GROUP BY status
ORDER BY stddev_monthly_fee DESC, status ASC
LIMIT 20;
