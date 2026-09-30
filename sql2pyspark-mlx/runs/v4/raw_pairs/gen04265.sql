WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label,
        COALESCE(merchant, 'OTHER') resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, merchant
    FROM base
    EXCEPT ALL
    SELECT id, merchant
    FROM transactions
    WHERE amount >= 2400
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id AND s.merchant = k.merchant
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.status,
        p2.balance,
        p2.account_type
    FROM verified a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        resolved_merchant,
        status,
        balance,
        account_type,
        AVG(balance) OVER (PARTITION BY account_type) account_type_avg_balance,
        AVG(balance) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) balance_avg_30d,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance NULLS FIRST) balance_dense_rank
    FROM with_dims
),
joined AS (
    -- Attach accounts attributes
    SELECT src.*, p.balance AS account_balance
    FROM with_windows src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT a.*, p2.balance AS account_balance_2
    FROM joined a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
)
-- Aggregate with conditional metrics
SELECT
    status,
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    CASE WHEN account_type_avg_balance >= 79121 THEN 'LARGE' ELSE 'SMALL' END AS account_type_avg_balance_tier,
    AVG(balance_avg_30d) AS avg_balance_avg_30d,
    SUM(CASE WHEN status = 'FROZEN' THEN account_type_avg_balance ELSE 0 END) AS frozen_account_type_avg_balance,
    ROUND(AVG(account_balance), 2) AS avg_account_balance,
    RANK() OVER (PARTITION BY status ORDER BY SUM(CASE WHEN status = 'FROZEN' THEN account_type_avg_balance ELSE 0 END) DESC) AS frozen_account_type_avg_balance_rank
FROM joined_transactions
GROUP BY status, DATE_TRUNC('QUARTER', txn_time), CASE WHEN account_type_avg_balance >= 79121 THEN 'LARGE' ELSE 'SMALL' END;
