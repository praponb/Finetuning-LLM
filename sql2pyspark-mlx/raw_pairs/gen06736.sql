WITH kept AS (
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    CASE WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'PRIORITY' WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'CORE' ELSE 'GROUP_A' END AS account_type_zone
FROM kept
