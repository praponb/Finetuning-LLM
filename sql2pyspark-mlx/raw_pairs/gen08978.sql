SELECT
    id,
    end_date,
    NTILE(5) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
    LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
    AVG(monthly_fee) OVER (ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5
FROM subscriptions
WHERE start_date > '2025-06-01'
