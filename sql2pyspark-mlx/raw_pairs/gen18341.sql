WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(merchant, ' - ', txn_type) AS merchant_txn_type
    FROM transactions
    WHERE amount <= 1200
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        d.balance,
        d.opened_date,
        d.account_type
    FROM valid a
    LEFT JOIN accounts d
        ON a.account_id = d.id
    WHERE d.account_type = 'SAVINGS'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time NULLS FIRST, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance) OVER (PARTITION BY account_id ORDER BY amount, id) AS prev_balance,
        LEAD(amount) OVER (PARTITION BY account_id ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS event_seq,
        LEAD(balance, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS next_balance
    FROM latest
)
-- Grouped metrics
SELECT
    merchant,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    COUNT(*) FILTER (WHERE account_type = 'LOAN') AS loan_rows,
    MAX(txn_time) AS ended_at,
    MAX(balance) FILTER (WHERE account_type = 'LOAN') AS loan_max_balance,
    COUNT(*) AS row_count,
    MIN(txn_time) AS started_at,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM metrics
GROUP BY merchant, activity_month
ORDER BY activity_month DESC
