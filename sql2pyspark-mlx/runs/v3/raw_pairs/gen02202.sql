SELECT
    account_type,
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    AVG(balance) avg_balance
FROM accounts
WHERE opened_date <= '2023-08-01'
GROUP BY account_type, status
ORDER BY account_type
