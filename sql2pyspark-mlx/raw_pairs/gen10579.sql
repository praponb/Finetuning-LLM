WITH enriched AS (
    SELECT
        *,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status, plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        RANK() OVER (ORDER BY monthly_fee DESC NULLS FIRST) monthly_fee_rank,
        ROW_NUMBER() OVER w_running seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date), plan ORDER BY end_date, id) event_seq
    FROM subscriptions
    WHERE YEAR(start_date) >= 2024
    WINDOW w_running AS (PARTITION BY plan, status ORDER BY start_date, id)
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.status,
    s.first_known_monthly_fee,
    s.monthly_fee_rank,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM enriched s;
