SELECT
    account_type,
    status,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) closed_count,
    MIN(balance) min_balance
FROM accounts
GROUP BY 1, 2
ORDER BY closed_count DESC;
