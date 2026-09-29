WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS next_monthly_fee,
        LAG(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS prev_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS event_seq
    FROM subscriptions
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    s.status,
    s.next_monthly_fee,
    s.event_seq,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_windows s
ORDER BY sum_subscription_monthly_fee;
