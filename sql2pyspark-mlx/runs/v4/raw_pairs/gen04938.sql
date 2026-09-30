WITH cleaned AS (
    SELECT
        *,
        CAST(is_flagged AS INT) is_flagged_flag,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
),
joined AS (
    SELECT
        e.*,
        a.status,
        a.opened_date
    FROM cleaned e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
base AS (
    SELECT *, TRIM(txn_type) clean_txn_type
    FROM joined
    WHERE merchant IS NOT NULL
),
joined_transactions AS (
    SELECT
        e.*,
        d.account_type,
        d.opened_date AS account_opened_date,
        d.balance
    FROM base e
    INNER JOIN accounts d
        ON e.account_id = d.id
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    is_flagged_flag,
    is_flagged_label,
    opened_date,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS txn_type_zone
FROM joined_transactions
ORDER BY opened_date DESC NULLS FIRST
