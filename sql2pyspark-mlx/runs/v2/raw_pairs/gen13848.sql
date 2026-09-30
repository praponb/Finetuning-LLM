SELECT
    id,
    customer_id,
    opened_date,
    balance,
    status,
    balance - LAG(balance, 2) OVER (PARTITION BY customer_id ORDER BY opened_date, id) balance_change,
    RANK() OVER (PARTITION BY status ORDER BY balance DESC) rnk
FROM accounts
WHERE opened_date < '2024-10-01';
