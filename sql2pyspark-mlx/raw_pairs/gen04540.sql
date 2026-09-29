WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        status,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE balance BETWEEN 49000 AND 79000
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        s.balance_int,
        latest.transaction_merchant,
        latest.transaction_amount
    FROM base s
    JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY balance_int DESC, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance_int,
        transaction_merchant,
        ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY balance_int DESC, id) AS seq_no,
        SUM(transaction_amount) OVER (PARTITION BY transaction_merchant ORDER BY balance_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_transaction_amount,
        SUM(transaction_amount) OVER (PARTITION BY transaction_merchant ORDER BY transaction_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_transaction_amount,
        AVG(transaction_amount) OVER (PARTITION BY status ORDER BY balance_int, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS transaction_amount_avg_3
    FROM latest
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY running_transaction_amount, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        balance_int,
        transaction_merchant,
        cumulative_transaction_amount,
        SUM(running_transaction_amount) OVER (PARTITION BY transaction_merchant ORDER BY running_transaction_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_transaction_amount,
        MAX(running_transaction_amount) OVER (PARTITION BY transaction_merchant ORDER BY running_transaction_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_transaction_amount,
        AVG(running_transaction_amount) OVER (PARTITION BY transaction_merchant ORDER BY running_transaction_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_transaction_amount,
        AVG(transaction_amount_avg_3) OVER (PARTITION BY transaction_merchant ORDER BY transaction_amount_avg_3, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS transaction_amount_avg_3_avg_2
    FROM deduped
)
SELECT
    id,
    cumulative_transaction_amount,
    running_sum_running_transaction_amount,
    running_max_running_transaction_amount,
    running_avg_running_transaction_amount,
    transaction_amount_avg_3_avg_2
FROM with_windows
