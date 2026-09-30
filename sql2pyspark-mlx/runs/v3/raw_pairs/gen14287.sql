SELECT
    id,
    status,
    balance,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS balance_dense_rank
FROM accounts
WHERE account_type = 'SAVINGS'
ORDER BY id DESC
