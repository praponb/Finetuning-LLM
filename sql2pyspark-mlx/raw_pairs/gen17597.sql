WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        ROUND(amount * 1.07, 2) amount_scaled,
        COALESCE(merchant, 'UNKNOWN') resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT merchant, account_id
    FROM valid
    EXCEPT
    SELECT merchant, account_id
    FROM transactions
    WHERE merchant NOT IN ('Shell', '7-Eleven')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.account_id = k.account_id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.status,
        p2.balance
    FROM verified a
    INNER JOIN accounts p2
        ON a.account_id = p2.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(amount, 0) amount_filled
    FROM joined
    WHERE txn_time > DATE '2023-05-25'
      AND status IS NOT NULL
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p.balance AS account_balance,
        p.account_type
    FROM base a
    LEFT JOIN accounts p
        ON a.account_id = p.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, amount_filled - amount_scaled amount_filled_minus_amount_scaled
    FROM joined_transactions
    WHERE account_type IS DISTINCT FROM 'CREDIT'
      AND account_balance BETWEEN 48000 AND 63000
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    SUM(amount_scaled) FILTER (WHERE txn_type = 'FEE') AS fee_total_amount_scaled,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', txn_time)
