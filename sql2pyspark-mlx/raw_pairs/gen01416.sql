WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-05-20 11:00:00'
      AND merchant IS DISTINCT FROM 'Shell'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY session_no ASC, id ASC) AS seq_no,
        NTILE(4) OVER (PARTITION BY txn_type ORDER BY is_credit ASC, id ASC) AS is_credit_bucket
    FROM sessions_transactions
),
remaining_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM enriched
    EXCEPT
    SELECT txn_type
    FROM transactions
    WHERE is_flagged = FALSE
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.txn_type = k.txn_type
),
base_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        seq_no * is_credit AS seq_no_times_is_credit,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM kept_rows
    WHERE txn_time > TIMESTAMP '2023-04-13 22:00:00'
      AND merchant IS NOT NULL
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.is_credit,
    s.is_new_burst,
    CASE WHEN merchant RLIKE '^(Starbucks|7-Eleven)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS merchant_zone,
    (SELECT MAX(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS max_transaction_amount
FROM base_transactions s
ORDER BY merchant_zone DESC NULLS FIRST;
