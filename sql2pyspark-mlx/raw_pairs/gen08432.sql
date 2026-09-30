WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(balance) AS balance_whole,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE account_type NOT IN ('CHECKING', 'CREDIT', 'SAVINGS')
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.txn_time,
        latest.amount
    FROM base s
    JOIN LATERAL (
            SELECT c.txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_balance,
        MAX(balance) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_balance,
        AVG(balance_whole) OVER (PARTITION BY customer_id ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_whole_avg_3
    FROM latest_match
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_merchant,
        top1.txn_type
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        opened_date,
        status,
        txn_time,
        last4_sum_balance,
        txn_type,
        MAX(last4_sum_balance) OVER (PARTITION BY txn_type, customer_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last4_sum_balance,
        MIN(last4_sum_balance) OVER (PARTITION BY txn_type, customer_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last4_sum_balance,
        AVG(last4_sum_balance) OVER (PARTITION BY txn_type, customer_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last4_sum_balance
    FROM latest_match_accounts
)
SELECT
    id,
    account_type,
    opened_date,
    status,
    txn_time,
    last4_sum_balance,
    txn_type,
    running_max_last4_sum_balance,
    running_min_last4_sum_balance,
    CASE WHEN account_type RLIKE '^(SAVINGS|CREDIT)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS account_type_zone
FROM with_windows_accounts
ORDER BY status DESC;
