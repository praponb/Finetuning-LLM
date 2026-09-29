WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(status) AS status_length,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE opened_date > DATE '2023-09-26'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.balance,
        e.opened_date,
        e.status,
        b.city,
        b.email
    FROM base e
    LEFT JOIN customers b
        ON e.customer_id = b.id
    WHERE b.city = 'New York'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        city,
        email
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('7-Eleven', 'Shell')
        )
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_txn_time,
        latest.txn_type
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    city,
    email,
    transaction_txn_time,
    txn_type,
    CASE WHEN txn_type RLIKE '^(CREDIT|DEBIT)$' THEN 'PRIORITY' WHEN txn_type RLIKE '^(REFUND|FEE)$' THEN 'CORE' ELSE 'GROUP_A' END AS txn_type_zone
FROM with_latest
ORDER BY transaction_txn_time DESC;
