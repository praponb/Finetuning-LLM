WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END is_7_eleven,
        COALESCE(merchant, 'NONE') resolved_merchant
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Lazada'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT txn_type, account_id
    FROM sessionized
    EXCEPT DISTINCT
    SELECT txn_type, account_id
    FROM transactions
    WHERE amount < 1400
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type AND s.account_id = k.account_id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.status,
        a.balance
    FROM reconciled s
    INNER JOIN accounts a
        ON s.account_id = a.id
    WHERE a.balance BETWEEN 23000 AND 77000
),
cleaned_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_7_eleven,
        prev_txn_time,
        is_new_burst,
        session_no,
        balance,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label,
        COALESCE(balance, 0) balance_filled
    FROM joined
    WHERE is_new_burst IS NULL
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        a.account_type,
        a.status
    FROM cleaned_transactions src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.txn_time,
        s.merchant,
        s.is_new_burst,
        s.session_no,
        s.balance,
        s.balance_filled,
        s.status,
        d.account_type AS account_account_type,
        d.opened_date
    FROM enriched s
    LEFT JOIN accounts d
        ON s.account_id = d.id
)
-- Join a dimension and aggregate
SELECT
    d.account_type,
    d.status AS account_status,
    COUNT(*) AS rows_total,
    ROUND(SUM(e.balance), 2) AS total_balance,
    ROUND(AVG(e.session_no), 2) AS avg_session_no,
    GROUPING_ID() AS level_id
FROM joined_transactions e
INNER JOIN accounts d ON e.account_id = d.id
GROUP BY CUBE (d.account_type, d.status)
ORDER BY total_balance NULLS LAST;
