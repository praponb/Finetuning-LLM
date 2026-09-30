SELECT account_type, status, balance_count
FROM (
    SELECT account_type, status, COUNT(balance) AS balance_count
    FROM accounts
    WHERE status != 'CLOSED'
    GROUP BY account_type, status
) AS s
WHERE balance_count < 9
ORDER BY balance_count DESC;
