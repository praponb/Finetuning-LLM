WITH valid_accounts AS (
    SELECT *, ROUND(balance * 0.85, 2) AS balance_scaled
    FROM accounts
    WHERE status NOT IN ('CLOSED', 'OPEN') AND status = 'CLOSED'
),
accounts_stats AS (
    SELECT
        status,
        QUARTER(opened_date) AS opened_quarter,
        ROUND(AVG(balance_scaled), 2) AS avg_balance_scaled,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM valid_accounts
    GROUP BY status, QUARTER(opened_date)
)
SELECT
    status,
    opened_quarter,
    avg_balance_scaled,
    account_type_list
FROM accounts_stats
WHERE avg_balance_scaled < 55
ORDER BY avg_balance_scaled DESC, status ASC, opened_quarter ASC
LIMIT 20
