SELECT
    id,
    account_type,
    balance,
    DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS dense_rnk,
    RANK() OVER (PARTITION BY account_type ORDER BY balance) AS rnk
FROM accounts;
