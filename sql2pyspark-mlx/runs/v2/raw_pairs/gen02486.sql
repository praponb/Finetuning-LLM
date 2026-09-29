WITH recent_accounts AS (
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE customer_id IS NULL AND (account_type = 'LOAN' OR status != 'CLOSED')
),
account_type_summary AS (
    SELECT account_type, SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM recent_accounts
    GROUP BY account_type
)
SELECT account_type, status_values
FROM account_type_summary
WHERE savings_balance > 37
ORDER BY account_type
