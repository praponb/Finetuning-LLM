SELECT
    status,
    SUM(monthly_fee) total_monthly_fee,
    COUNT(*) cnt,
    ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
GROUP BY status
ORDER BY cnt DESC
