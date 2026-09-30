WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label_2,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE opened_date >= DATE '2023-09-26'
      AND account_type = 'CREDIT'
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, b.segment
    FROM cleaned a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_type, status
    FROM with_dims
    EXCEPT ALL
    SELECT account_type, status
    FROM accounts
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.account_type = k.account_type AND s.status = k.status
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= '2024-05-05'
        )
)
SELECT
    account_type,
    balance,
    opened_date,
    status,
    opened_date_label,
    opened_date_label_2
FROM eligible
ORDER BY status DESC;
