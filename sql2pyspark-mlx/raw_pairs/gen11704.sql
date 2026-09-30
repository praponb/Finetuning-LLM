WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS next_balance,
        LAG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS seq_no
    FROM accounts
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_balance) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), status) AS opened_date_period_avg_prev_balance,
        LAG(next_balance, 1, 0) OVER (ORDER BY next_balance, id) AS prev_next_balance,
        CUME_DIST() OVER w_part AS balance_cume_dist,
        MAX(next_balance) OVER (PARTITION BY account_type) AS account_type_max_next_balance,
        NTILE(3) OVER (PARTITION BY account_type, customer_id ORDER BY next_balance DESC, id) AS next_balance_bucket
    FROM not_flagged
    WHERE next_balance > 72000
    WINDOW w_part AS (PARTITION BY status, customer_id ORDER BY balance DESC)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_txn_type
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    dim.country,
    s.status,
    COUNT(*) AS rows_total,
    AVG(s.balance) AS avg_balance,
    MAX(s.prev_balance) AS max_prev_balance,
    GROUPING_ID() AS level_id
FROM latest_match s
LEFT JOIN customers dim ON s.customer_id = dim.id
GROUP BY CUBE (dim.segment, dim.country, s.status)
ORDER BY status DESC
