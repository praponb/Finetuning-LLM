SELECT
    *,
    LEAD(monthly_fee) OVER w_part AS next_monthly_fee,
    ROW_NUMBER() OVER w_part AS row_seq,
    PERCENT_RANK() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE', 'PAUSED')
WINDOW w_part AS (PARTITION BY status ORDER BY end_date, id);
