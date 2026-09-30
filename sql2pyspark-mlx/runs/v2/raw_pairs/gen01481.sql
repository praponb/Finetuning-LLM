SELECT
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
GROUP BY status
ORDER BY status_values, status DESC
LIMIT 10
