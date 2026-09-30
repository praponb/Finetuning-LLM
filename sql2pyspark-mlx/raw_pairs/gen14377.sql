WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(balance) OVER w_grp AS last3_avg_balance,
        MIN(balance) OVER w_grp AS last3_min_balance,
        SUM(balance) OVER w_grp AS last3_sum_balance,
        FIRST_VALUE(status, TRUE) OVER w_recent AS first_known_status
    FROM accounts
    WINDOW w_grp AS (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.last3_avg_balance,
    s.last3_min_balance,
    s.last3_sum_balance,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM metrics s
