SELECT
    QUARTER(start_date) AS start_quarter,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    SUM(SUM(monthly_fee)) OVER (ORDER BY QUARTER(start_date)) AS cumulative_monthly_fee
FROM subscriptions
WHERE monthly_fee > 110
GROUP BY QUARTER(start_date)
ORDER BY cnt DESC, start_quarter ASC
LIMIT 5
