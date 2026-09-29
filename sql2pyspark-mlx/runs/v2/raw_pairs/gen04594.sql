SELECT
    QUARTER(start_date) AS start_quarter,
    AVG(monthly_fee) AS avg_monthly_fee,
    MAX(start_date) AS last_start_date,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY QUARTER(start_date);
