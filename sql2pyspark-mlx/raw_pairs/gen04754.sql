WITH recent_accounts AS (
    SELECT *, LOWER(status) AS status_lower
    FROM accounts
    WHERE account_type NOT IN ('LOAN', 'CREDIT', 'SAVINGS') OR balance > 18000
),
agg AS (
    SELECT account_type, MIN(opened_date) AS min_opened_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM recent_accounts
    GROUP BY account_type
)
SELECT *
FROM agg
ORDER BY status_values, account_type
LIMIT 20
