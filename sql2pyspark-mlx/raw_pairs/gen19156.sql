WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount < 3900
        )
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        x.txn_type,
        x.txn_time
    FROM kept s
    JOIN LATERAL (
            SELECT c.txn_type, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND NOT x.is_flagged
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.txn_time
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) seq_no,
        LAST_VALUE(txn_type) IGNORE NULLS OVER w_part last_known_txn_type
    FROM eligible
    WINDOW w_part AS (PARTITION BY customer_id, account_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    id,
    last_known_txn_type,
    SUM(seq_no) OVER (ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_seq_no,
    FIRST_VALUE(seq_no, TRUE) OVER (PARTITION BY txn_type, account_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_seq_no,
    AVG(seq_no) OVER (PARTITION BY account_type, customer_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) seq_no_avg_2_prior
FROM metrics
