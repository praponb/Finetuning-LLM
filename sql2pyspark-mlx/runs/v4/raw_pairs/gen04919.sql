WITH kept AS (
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'DEBIT'
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
kept_accounts AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
eligible AS (
    SELECT s.*
    FROM kept_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.amount <= 1800
        )
)
SELECT
    id,
    account_type,
    balance,
    opened_date,
    CASE WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS account_type_zone
FROM eligible
ORDER BY opened_date ASC NULLS FIRST
