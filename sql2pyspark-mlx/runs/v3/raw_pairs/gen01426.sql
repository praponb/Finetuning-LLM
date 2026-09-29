WITH recent_accounts AS (
    SELECT *, SUBSTRING(account_type, 1, 3) account_type_prefix
    FROM accounts
    WHERE account_type = 'CHECKING'
),
status_summary AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(account_type)) account_type_list, MAX(opened_date) latest_opened_date
    FROM recent_accounts
    GROUP BY status
)
SELECT status, account_type_list, latest_opened_date
FROM status_summary;
