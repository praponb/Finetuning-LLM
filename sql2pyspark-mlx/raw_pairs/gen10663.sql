WITH not_flagged AS (
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant NOT IN ('Shell', 'Starbucks', 'Lazada')
        )
)
SELECT
    id,
    account_type,
    opened_date,
    status,
    CASE WHEN account_type RLIKE '^(CREDIT|CHECKING)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS account_type_zone
FROM not_flagged
ORDER BY status DESC NULLS FIRST
