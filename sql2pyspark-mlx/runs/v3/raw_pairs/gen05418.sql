SELECT account_type, min_balance
FROM (
    SELECT account_type, MIN(balance) AS min_balance
    FROM accounts
    GROUP BY account_type
) AS x
WHERE min_balance > 63000;
