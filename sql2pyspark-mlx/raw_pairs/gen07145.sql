SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    end_date,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
    PERCENT_RANK() OVER (ORDER BY end_date DESC) AS end_date_pct_rank
FROM subscriptions
