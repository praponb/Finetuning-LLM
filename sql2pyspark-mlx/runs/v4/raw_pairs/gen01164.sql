WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        FIRST_VALUE(balance, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance_2
    FROM accounts
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        first_known_balance,
        first_known_balance_2,
        STDDEV_SAMP(first_known_balance_2) OVER (PARTITION BY status) AS status_stddev_samp_first_known_balance_2,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY balance, id) AS balance_bucket,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY first_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
    FROM enriched
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_txn_type
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_merchant
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM latest_match_accounts
    EXCEPT ALL
    SELECT account_type, id
    FROM accounts
    WHERE balance BETWEEN 44000 AND 58000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match_accounts s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type AND s.id = k.id
)
SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    first_known_balance,
    status_stddev_samp_first_known_balance_2,
    balance_bucket,
    running_balance
FROM reconciled
ORDER BY balance DESC NULLS LAST
