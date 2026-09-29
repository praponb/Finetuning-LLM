SELECT
    YEAR(end_date) end_year,
    SUM(monthly_fee) total_monthly_fee,
    COUNT(*) cnt,
    RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS monthly_fee_rank
FROM subscriptions
GROUP BY YEAR(end_date);
