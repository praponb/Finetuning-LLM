SELECT
    id,
    status,
    end_date,
    monthly_fee,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_3
FROM subscriptions
WHERE customer_id IS NOT NULL
ORDER BY id;
