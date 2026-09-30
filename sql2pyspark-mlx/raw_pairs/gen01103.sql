SELECT
    id,
    customer_id,
    start_date,
    end_date,
    status,
    PERCENT_RANK() OVER (PARTITION BY customer_id, status ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank,
    AVG(monthly_fee) OVER w_running AS customer_id_avg_monthly_fee
FROM subscriptions
WHERE status IN ('EXPIRED', 'CANCELLED', 'PAUSED')
WINDOW w_running AS (PARTITION BY customer_id);
