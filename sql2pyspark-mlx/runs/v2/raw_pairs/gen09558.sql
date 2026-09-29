SELECT
    status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(balance) - MIN(balance) AS balance_range,
    MIN(balance) AS min_balance
FROM accounts
WHERE status IN ('FROZEN', 'CLOSED', 'OPEN')
GROUP BY status
ORDER BY balance_range DESC
