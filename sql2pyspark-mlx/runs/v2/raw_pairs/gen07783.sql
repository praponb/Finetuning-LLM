SELECT
    status,
    COUNT(DISTINCT monthly_fee) AS unique_plans,
    SUM(monthly_fee) AS total_monthly_fee,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 120 AND end_date BETWEEN '2023-08-26' AND '2025-04-29'
GROUP BY status
ORDER BY total_monthly_fee DESC, status
