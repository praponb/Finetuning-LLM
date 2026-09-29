WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(merchant, '/', txn_type) AS merchant_txn_type,
        LOWER(txn_type) AS txn_type_lower
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-11-01 12:00:00'
      AND merchant IS NOT NULL
      AND txn_type IN ('FEE', 'DEBIT')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY txn_type) AS txn_type_count_amount,
        LEAD(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS next_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id, merchant), 4) AS amount_share
    FROM base
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        amount - txn_type_count_amount AS amount_minus_txn_type_count_amount,
        CEIL(amount) AS amount_whole
    FROM with_windows
    WHERE txn_time >= TIMESTAMP '2024-02-08 16:00:00'
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_id
    FROM valid
    EXCEPT
    SELECT account_id
    FROM transactions
    WHERE account_id IN (4, 27, 36, 38)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.account_id = k.account_id
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    is_flagged,
    merchant_txn_type,
    txn_type_lower,
    txn_type_count_amount
FROM reconciled
ORDER BY txn_type DESC
