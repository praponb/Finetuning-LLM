SELECT
    account_type,
    CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'priority' ELSE 'other' END AS status_group,
    MAX(balance) - MIN(balance) AS balance_range,
    COUNT(balance) AS balance_count
FROM accounts
GROUP BY account_type, status_group
ORDER BY balance_range DESC, account_type, status_group
LIMIT 3;
