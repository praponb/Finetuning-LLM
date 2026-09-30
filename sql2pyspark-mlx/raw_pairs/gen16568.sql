WITH with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        latest.txn_type
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        balance,
        NTILE(5) OVER (PARTITION BY account_type ORDER BY balance, id) AS balance_bucket,
        ROW_NUMBER() OVER w_part AS row_seq,
        RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_rank,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS event_seq,
        SUM(balance) OVER (PARTITION BY txn_type) AS txn_type_sum_balance
    FROM with_last
    WINDOW w_part AS (ORDER BY balance DESC, id)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.account_id IN (24, 29, 32, 35)
        )
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_amount
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(balance_bucket, txn_type_sum_balance) AS greatest_balance_bucket_txn_type_sum_balance
    FROM with_latest
    WHERE event_seq <> 6
)
SELECT
    id,
    account_type,
    balance_bucket,
    balance_rank,
    txn_type_sum_balance,
    greatest_balance_bucket_txn_type_sum_balance
FROM cleaned;
