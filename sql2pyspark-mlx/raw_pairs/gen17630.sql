WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(account_type) AS account_type_lower,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE balance >= 55000
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, account_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
cleaned_accounts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(account_type) AS clean_account_type,
        UPPER(account_type) AS account_type_upper
    FROM latest
    WHERE account_type = 'SAVINGS'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount IS NOT NULL
        )
)
SELECT
    id,
    account_type,
    clean_account_type,
    account_type_upper
FROM kept
ORDER BY id NULLS FIRST, account_type
