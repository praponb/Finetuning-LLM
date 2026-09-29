SELECT
    id,
    customer_id,
    opened_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date) dense_rnk
FROM accounts
WHERE customer_id IS NOT NULL
