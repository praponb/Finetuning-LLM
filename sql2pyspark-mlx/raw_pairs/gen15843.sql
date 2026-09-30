SELECT
    account_type,
    MIN(balance) AS min_balance,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
    COUNT(*) FILTER (WHERE status = 'FROZEN') AS frozen_rows
FROM accounts
GROUP BY account_type
ORDER BY min_balance
