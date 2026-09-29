WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        MIN(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_balance,
        COUNT(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_balance,
        MAX(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_balance,
        SUM(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) seq_no
    FROM accounts
    WHERE balance <= 38000
)
SELECT
    id,
    last4_min_balance,
    last4_count_balance,
    last4_max_balance,
    last4_sum_balance
FROM metrics
ORDER BY last4_sum_balance DESC NULLS FIRST
