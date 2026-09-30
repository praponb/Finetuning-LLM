SELECT
    status,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee,
    COUNT(*) AS row_count,
    COUNT(DISTINCT monthly_fee) AS unique_customers
FROM subscriptions
WHERE customer_id = 35 AND status IN ('EXPIRED', 'ACTIVE')
GROUP BY status
ORDER BY 3;
