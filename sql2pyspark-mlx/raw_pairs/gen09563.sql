SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    SUM(monthly_fee) OVER w_recent AS running_monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY YEAR(start_date), customer_id), 4) AS monthly_fee_share,
    LAG(monthly_fee, 2, 0) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS prev_monthly_fee
FROM subscriptions
WINDOW w_recent AS (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
