WITH eligible AS (
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
latest_match AS (
    SELECT
        s.*,
        x.transaction_txn_time,
        x.transaction_txn_type
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    SELECT
        id,
        balance,
        status,
        transaction_txn_time,
        transaction_txn_type,
        CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END AS is_checking,
        COALESCE(status, 'N/A') AS status_clean
    FROM latest_match
    WHERE TO_DATE(transaction_txn_time) >= '2023-10-31'
),
latest_match_accounts AS (
    SELECT
        s.*,
        x.merchant,
        x.transaction_amount
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        *,
        AVG(transaction_amount) OVER (PARTITION BY transaction_txn_type ORDER BY is_checking, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_transaction_amount,
        MIN(transaction_amount) OVER (PARTITION BY transaction_txn_type ORDER BY is_checking, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_transaction_amount,
        COUNT(transaction_amount) OVER (PARTITION BY transaction_txn_type ORDER BY is_checking, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_transaction_amount,
        MAX(transaction_amount) OVER (PARTITION BY transaction_txn_type ORDER BY is_checking, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_transaction_amount
    FROM latest_match_accounts
)
SELECT
    id,
    balance,
    status,
    status_clean,
    transaction_amount,
    running_avg_transaction_amount,
    running_min_transaction_amount,
    running_count_transaction_amount,
    running_max_transaction_amount
FROM enriched
ORDER BY running_min_transaction_amount DESC NULLS LAST;
