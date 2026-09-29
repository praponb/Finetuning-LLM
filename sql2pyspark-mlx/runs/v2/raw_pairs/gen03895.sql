SELECT
    id,
    status,
    opened_date,
    balance,
    customer_id,
    LEAD(balance, 1, 0) OVER (PARTITION BY status ORDER BY opened_date, id) next_balance,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) rn
FROM accounts
