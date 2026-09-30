SELECT
    QUARTER(end_date) AS end_quarter,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
GROUP BY QUARTER(end_date)
ORDER BY end_quarter DESC;
