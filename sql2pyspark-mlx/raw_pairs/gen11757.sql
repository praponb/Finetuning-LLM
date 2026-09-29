SELECT
    customer_id,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee,
    COUNT(*) AS num_subscriptions,
    ROUND((100 * SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_premium
FROM subscriptions
GROUP BY customer_id
ORDER BY pct_premium
