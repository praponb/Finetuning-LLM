SELECT
    id,
    balance,
    customer_id,
    status
FROM accounts
WHERE balance >= (SELECT AVG(balance) FROM accounts)
ORDER BY balance DESC
