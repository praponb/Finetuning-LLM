SELECT
    id,
    status,
    balance,
    RANK() OVER (PARTITION BY status ORDER BY balance) AS rnk
FROM accounts
