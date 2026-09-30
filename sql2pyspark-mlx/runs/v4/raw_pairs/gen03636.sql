SELECT
    id,
    balance,
    customer_id,
    opened_date,
    DENSE_RANK() OVER (ORDER BY balance DESC) AS balance_dense_rank,
    LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS next_balance
FROM accounts
