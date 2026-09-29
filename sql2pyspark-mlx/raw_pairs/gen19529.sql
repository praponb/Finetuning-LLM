WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(account_type) clean_account_type
    FROM accounts
    WHERE status IN ('CLOSED', 'OPEN', 'FROZEN')
      AND status IS DISTINCT FROM 'CLOSED'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        clean_account_type,
        COALESCE(balance, 0) balance_filled,
        DATE_TRUNC('month', opened_date) opened_date_period
    FROM filtered
    WHERE opened_date >= TO_DATE('2023-08-18')
      AND YEAR(opened_date) = 2025
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.opened_date,
        s.status,
        s.balance_filled,
        x.merchant,
        x.transaction_txn_time
    FROM base s
    JOIN LATERAL (
            SELECT c.merchant, c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        status,
        balance_filled,
        transaction_txn_time,
        SUM(balance_filled) OVER w_part last4_sum_balance_filled,
        MIN(balance_filled) OVER w_part last4_min_balance_filled,
        AVG(balance_filled) OVER w_part last4_avg_balance_filled
    FROM with_latest
    WINDOW w_part AS (PARTITION BY merchant ORDER BY transaction_txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    account_type,
    DATE_TRUNC('QUARTER', transaction_txn_time) AS quarter_start,
    CASE WHEN last4_min_balance_filled >= 78 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_balance_filled_tier,
    SUM(CASE WHEN account_type = 'CREDIT' THEN last4_avg_balance_filled ELSE 0 END) AS credit_last4_avg_balance_filled,
    COUNT(*) FILTER (WHERE account_type = 'LOAN') AS loan_rows,
    APPROX_PERCENTILE(balance_filled, ARRAY(0.25, 0.5, 0.75)) AS balance_filled_quartiles,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    MIN(transaction_txn_time) AS first_transaction_txn_time
FROM with_windows
GROUP BY account_type, quarter_start, last4_min_balance_filled_tier
