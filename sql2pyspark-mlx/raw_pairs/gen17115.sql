WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        balance,
        status,
        SUBSTRING(account_type, 1, 4) AS account_type_prefix,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-08-07')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        status,
        account_type_prefix,
        resolved_status,
        CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'Group A' ELSE 'Other' END AS status_group,
        COALESCE(status, 'NONE') AS status_clean
    FROM cleaned
    WHERE status IS NOT NULL
      AND balance < 65000
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_txn_time,
        x.merchant
    FROM ranked s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    status,
    account_type_prefix,
    resolved_status,
    status_group,
    status_clean,
    row_num,
    transaction_txn_time,
    merchant,
    CASE WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'GROUP_B' WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'CORE' ELSE 'EXTENDED' END AS merchant_zone
FROM latest_match
