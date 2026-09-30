WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM transactions
    WHERE merchant IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY is_credit, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        d.balance,
        d.account_type,
        d.status
    FROM ranked a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
base_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_credit,
        rn,
        balance,
        status,
        CAST(txn_time AS DATE) AS txn_time_day
    FROM with_dims
    WHERE merchant IS NOT NULL
      AND txn_time >= TIMESTAMP '2023-05-06 13:00:00'
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_credit,
    rn
FROM base_transactions
ORDER BY is_credit DESC
