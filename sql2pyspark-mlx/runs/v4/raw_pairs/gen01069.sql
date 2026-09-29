WITH enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        p.status,
        p.opened_date,
        p.account_type
    FROM transactions src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT merchant
    FROM enriched
    EXCEPT ALL
    SELECT merchant
    FROM transactions
    WHERE txn_type IN ('REFUND', 'CREDIT', 'DEBIT')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.merchant = k.merchant
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
        COUNT(amount) OVER (PARTITION BY account_id) AS account_id_count_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status, account_id), 4) AS amount_share
    FROM kept_rows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(amount_share) OVER (PARTITION BY account_type) AS account_type_stddev_samp_amount_share,
        MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount
    FROM with_windows
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        b.opened_date AS account_opened_date,
        b.account_type AS account_account_type,
        b.balance
    FROM metrics src
    LEFT JOIN accounts b
        ON src.account_id = b.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        p.account_type AS account_account_type_2,
        p.opened_date AS account_opened_date_2,
        p.balance AS account_balance
    FROM with_dims src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_part AS amount_rank,
        COUNT(balance) OVER (PARTITION BY account_account_type_2) AS account_account_type_2_count_balance
    FROM with_dims_transactions
    WINDOW w_part AS (ORDER BY amount DESC)
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.merchant,
    s.is_flagged,
    s.status,
    s.opened_date,
    s.account_type,
    s.amount_count_30d,
    CASE WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS txn_type_zone,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM with_windows_transactions s
ORDER BY txn_type_zone DESC NULLS LAST, opened_date
