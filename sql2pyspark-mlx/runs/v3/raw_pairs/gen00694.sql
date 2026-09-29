SELECT
    account_type,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    MAX(opened_date) AS latest_opened_date,
    SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance
FROM accounts
GROUP BY account_type
HAVING frozen_balance > 55
