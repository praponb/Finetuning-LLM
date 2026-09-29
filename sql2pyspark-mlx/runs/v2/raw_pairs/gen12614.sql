WITH base AS (
    SELECT *
    FROM accounts
    WHERE status IS NOT NULL AND opened_date > '2024-11-01'
),
summary AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM base
    GROUP BY status
)
SELECT status, account_type_list
FROM summary
