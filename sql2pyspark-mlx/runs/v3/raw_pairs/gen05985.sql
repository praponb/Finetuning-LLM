SELECT account_type, status, min_balance
FROM (
    SELECT account_type, status, MIN(balance) AS min_balance
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT')
    GROUP BY account_type, status
) AS s
WHERE min_balance >= 39000
ORDER BY min_balance, account_type, status
LIMIT 5;
