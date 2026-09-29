SELECT
    id,
    customer_id,
    MAX(balance) OVER w_ordered AS running_max_balance,
    AVG(balance) OVER w_ordered AS running_avg_balance,
    COUNT(balance) OVER w_ordered AS running_count_balance,
    RANK() OVER (PARTITION BY status ORDER BY balance NULLS FIRST) AS balance_rank
FROM accounts
WINDOW w_ordered AS (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
