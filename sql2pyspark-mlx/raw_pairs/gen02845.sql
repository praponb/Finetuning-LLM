WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-08-23 15:00:00'
      AND merchant IS NOT NULL
),
joined AS (
    -- Attach accounts attributes
    SELECT src.*, a.opened_date
    FROM filtered src
    JOIN accounts a
        ON src.account_id = a.id
    WHERE a.account_type IN ('LOAN', 'CHECKING', 'SAVINGS')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT txn_type, account_id
    FROM joined
    INTERSECT ALL
    SELECT txn_type, account_id
    FROM transactions
    WHERE is_flagged = TRUE
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.txn_type = k.txn_type AND s.account_id = k.account_id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.txn_time,
        s.merchant,
        s.is_flagged_label,
        s.opened_date,
        d.status,
        d.opened_date AS account_opened_date,
        d.balance
    FROM kept_rows s
    JOIN accounts d
        ON s.account_id = d.id
)
SELECT
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.opened_date,
    s.status,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM with_dims s
ORDER BY txn_type DESC
