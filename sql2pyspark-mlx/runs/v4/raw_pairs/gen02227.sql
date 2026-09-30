SELECT
    *,
    LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
    NTILE(3) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket
FROM subscriptions
