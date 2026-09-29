SELECT
    id,
    account_type,
    balance,
    RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS rnk
FROM accounts
ORDER BY id;
