WITH cleaned AS (
    SELECT
        *,
        CASE WHEN account_type IN ('SAVINGS', 'LOAN') THEN 'Group A' ELSE 'Other' END AS account_type_group,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE customer_id IS NOT NULL
)
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions c
        WHERE c.account_id = s.id
          AND c.txn_time < s.opened_date
    )
