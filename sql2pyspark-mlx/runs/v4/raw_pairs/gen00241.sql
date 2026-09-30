SELECT
    id,
    plan,
    monthly_fee,
    customer_id,
    end_date,
    SUM(monthly_fee) OVER (PARTITION BY plan) AS plan_sum_monthly_fee,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_3
FROM subscriptions
WHERE status = 'EXPIRED';
