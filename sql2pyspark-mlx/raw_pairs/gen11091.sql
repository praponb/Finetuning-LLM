WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance) OVER (PARTITION BY status ORDER BY balance, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY status ORDER BY balance, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS seq_no
    FROM accounts
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (5, 17, 19, 34)
        )
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        s.prev_balance,
        top1.transaction_txn_time
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        prev_balance,
        LAG(balance) OVER w_ordered AS prev_balance_2,
        LAG(prev_balance) OVER w_ordered AS prev_prev_balance,
        LEAD(prev_balance) OVER w_ordered AS next_prev_balance,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY account_type, status ORDER BY transaction_txn_time, id)
),
not_flagged_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged = TRUE
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant = 'Shell'
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(row_seq) OVER w_hist AS row_seq_avg_2_prior,
        LEAD(row_seq, 1) OVER (ORDER BY prev_balance_2, id) AS next_row_seq,
        FIRST_VALUE(prev_balance_2) IGNORE NULLS OVER (ORDER BY prev_prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_balance_2,
        SUM(prev_balance) OVER w_grp AS cumulative_prev_balance,
        LEAD(prev_balance_2, 1, 0) OVER (ORDER BY prev_prev_balance, id) AS next_prev_balance_2
    FROM eligible
    WHERE row_seq <= 4
    WINDOW w_hist AS (ORDER BY prev_prev_balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_grp AS (ORDER BY prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_balance >= 69000 THEN 'LARGE' ELSE 'SMALL' END AS prev_balance_tier,
    CASE WHEN cumulative_prev_balance >= 317260 THEN 'HIGH' ELSE 'LOW' END AS cumulative_prev_balance_tier,
    COUNT(*) AS event_count,
    SUM(next_row_seq) AS total_next_row_seq,
    ROUND(AVG(next_prev_balance), 2) AS avg_next_prev_balance
FROM enriched
GROUP BY ROLLUP (CASE WHEN prev_balance >= 69000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_prev_balance >= 317260 THEN 'HIGH' ELSE 'LOW' END)
