SELECT
    status,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE start_date BETWEEN '2024-08-08' AND '2024-12-11'
GROUP BY status
ORDER BY pct_of_total DESC;
