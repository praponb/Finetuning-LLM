WITH ValidAccounts AS (
    SELECT *
    FROM accounts
    WHERE MONTH(opened_date) IN (7, 8, 10) AND status = 'OPEN'
),
agg AS (
    SELECT account_type, COUNT(DISTINCT balance) AS unique_statuss, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM ValidAccounts
    GROUP BY account_type
)
SELECT account_type, unique_statuss, status_list
FROM agg
WHERE unique_statuss > 4
ORDER BY account_type
LIMIT 10
