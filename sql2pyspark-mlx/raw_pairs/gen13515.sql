WITH with_windows AS (
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        LAG(monthly_fee) OVER w_ordered AS prev_monthly_fee,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM subscriptions
    WINDOW w_ordered AS (PARTITION BY status, customer_id ORDER BY end_date, id)
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.prev_monthly_fee,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_windows s
ORDER BY customer_id DESC, id;
