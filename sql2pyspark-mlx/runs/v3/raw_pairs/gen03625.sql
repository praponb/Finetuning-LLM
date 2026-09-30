SELECT
    id,
    customer_id,
    monthly_fee,
    plan,
    end_date,
    RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) AS monthly_fee_rank,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_3
FROM subscriptions
WHERE customer_id IN (7, 13, 15);
