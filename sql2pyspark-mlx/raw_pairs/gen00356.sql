WITH with_dims AS (
    -- Attach customers attributes
    SELECT a.*, c.city
    FROM accounts a
    JOIN customers c
        ON a.customer_id = c.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        FIRST_VALUE(balance) IGNORE NULLS OVER w_ordered AS first_known_balance,
        LEAD(balance, 2) OVER w_recent AS next_balance,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY balance NULLS LAST) AS balance_dense_rank
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY MONTH(opened_date), status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY account_type ORDER BY balance, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount >= 1700
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance_dense_rank) OVER (PARTITION BY status, account_type ORDER BY next_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_dense_rank_avg_3,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY status ORDER BY first_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        NTILE(5) OVER (PARTITION BY status ORDER BY first_known_balance DESC, id) AS first_known_balance_bucket
    FROM not_flagged
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        balance,
        status,
        first_known_balance,
        next_balance,
        balance_dense_rank,
        LAG(balance_dense_rank) OVER (PARTITION BY status, account_type ORDER BY balance, id) AS prev_balance_dense_rank,
        LEAD(balance_dense_rank) OVER (PARTITION BY status, account_type ORDER BY balance, id) AS next_balance_dense_rank,
        LAG(next_balance) OVER (PARTITION BY status, account_type ORDER BY balance, id) AS prev_next_balance,
        LEAD(next_balance) OVER (PARTITION BY status, account_type ORDER BY balance, id) AS next_next_balance,
        ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance, id) AS seq_no
    FROM scored
    WHERE first_known_balance <= 22000
)
SELECT
    id,
    balance,
    first_known_balance,
    next_balance,
    prev_balance_dense_rank,
    next_balance_dense_rank,
    next_next_balance,
    seq_no
FROM metrics
