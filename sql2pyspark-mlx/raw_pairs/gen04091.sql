SELECT
    id,
    customer_id,
    balance,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY balance) AS dense_rnk
FROM accounts
WHERE status = 'CLOSED';
