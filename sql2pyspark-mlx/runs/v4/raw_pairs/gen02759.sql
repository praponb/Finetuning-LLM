WITH base AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        CONCAT(merchant, '/', txn_type) AS merchant_txn_type
    FROM transactions
    WHERE txn_time >= DATE '2024-01-29'
      AND merchant IS NOT NULL
),
filtered AS (
    SELECT
        id,
        account_id,
        is_flagged,
        is_flagged_flag,
        TO_DATE(txn_time) AS txn_time_date
    FROM base
    WHERE txn_time >= TIMESTAMP '2023-11-20 19:00:00'
      AND is_flagged_flag IS NOT NULL
),
filtered_transactions AS (
    SELECT
        id,
        is_flagged,
        is_flagged_flag,
        txn_time_date,
        DATE_FORMAT(txn_time_date, 'yyyy-MM-dd') AS txn_time_date_label
    FROM filtered
    WHERE is_flagged_flag <= 53
)
SELECT
    id,
    is_flagged,
    txn_time_date,
    txn_time_date_label
FROM filtered_transactions
ORDER BY id DESC NULLS FIRST
