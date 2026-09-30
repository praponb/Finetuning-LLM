WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LOWER(status) AS status_lower,
        CONCAT_WS('-', status, account_type) AS status_account_type_key
    FROM accounts
    WHERE status IS NOT NULL
      AND status IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.country,
        b.email,
        b.name
    FROM base s
    INNER JOIN customers b
        ON s.customer_id <=> b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status_account_type_key,
        country,
        email,
        name,
        COALESCE(balance, 0) AS balance_filled
    FROM joined
    WHERE opened_date >= DATE '2023-03-23'
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status_account_type_key,
        s.country,
        s.name,
        s.balance_filled,
        x.transaction_txn_time
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    country,
    transaction_txn_time,
    CASE WHEN country RLIKE '^(TH|DE)$' THEN 'PRIORITY' WHEN country RLIKE '^(US|SG)$' THEN 'GROUP_B' ELSE 'CORE' END AS country_zone
FROM with_latest
ORDER BY country DESC NULLS FIRST;
