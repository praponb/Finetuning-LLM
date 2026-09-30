WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        UPPER(account_type) account_type_upper
    FROM accounts
    WHERE account_type = 'LOAN'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        account_type_upper
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date NULLS LAST, id) = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.status,
        latest.transaction_txn_time,
        latest.transaction_txn_type
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        status,
        transaction_txn_time,
        transaction_txn_type,
        INITCAP(TRIM(status)) status_title
    FROM latest_match
    WHERE opened_date BETWEEN '2024-07-02' AND '2025-02-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_txn_type, customer_id ORDER BY transaction_txn_time DESC NULLS LAST, id) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.status,
    s.transaction_txn_time,
    s.transaction_txn_type,
    s.status_title,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM latest s
ORDER BY transaction_txn_time DESC NULLS LAST
