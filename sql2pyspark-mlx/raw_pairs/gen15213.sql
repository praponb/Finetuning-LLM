SELECT
    id,
    account_type,
    balance,
    status
FROM (
    SELECT
        id,
        account_type,
        balance,
        status,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rnk
    FROM accounts
) AS r
WHERE rnk = 1
ORDER BY balance
