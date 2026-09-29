WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(balance) OVER (PARTITION BY account_type) AS account_type_stddev_samp_balance,
        LAG(balance, 2, 0) OVER (PARTITION BY account_type ORDER BY balance, id) AS prev_balance,
        LEAD(balance, 1) OVER (PARTITION BY status ORDER BY balance, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS event_seq,
        SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM accounts
    WHERE customer_id IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.account_type_stddev_samp_balance,
    s.prev_balance,
    s.cumulative_balance,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM scored s
ORDER BY customer_id DESC NULLS FIRST
