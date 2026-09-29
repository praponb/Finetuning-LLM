SELECT
    id,
    plan,
    monthly_fee,
    end_date,
    MIN(monthly_fee) OVER (PARTITION BY plan) AS plan_min_monthly_fee,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_3
FROM subscriptions;
