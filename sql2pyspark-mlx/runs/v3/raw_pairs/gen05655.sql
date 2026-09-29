SELECT
    id,
    end_date,
    monthly_fee,
    customer_id,
    ROUND(AVG(monthly_fee) OVER (ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_7,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 3) AS monthly_fee_share
FROM subscriptions;
