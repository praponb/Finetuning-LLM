SELECT
    *,
    SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
    LAST_VALUE(status) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
    LEAD(balance, 2) OVER w_grp AS next_balance,
    CUME_DIST() OVER (PARTITION BY status ORDER BY balance NULLS FIRST) AS balance_cume_dist,
    DENSE_RANK() OVER w_hist AS balance_dense_rank
FROM accounts
WINDOW w_grp AS (ORDER BY opened_date, id), w_hist AS (PARTITION BY customer_id, status ORDER BY balance DESC)
