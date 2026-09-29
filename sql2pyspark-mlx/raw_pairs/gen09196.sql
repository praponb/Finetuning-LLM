SELECT
    id,
    customer_id,
    balance,
    RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS balance_rank
FROM accounts
WHERE customer_id = 4
ORDER BY balance DESC
