SELECT
    id,
    customer_id,
    opened_date,
    balance,
    balance - LAG(balance, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS balance_change,
    LAG(opened_date, 1) OVER (ORDER BY opened_date, id) AS prev_opened_date
FROM accounts
ORDER BY customer_id DESC;
