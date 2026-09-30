SELECT
    id,
    customer_id,
    opened_date,
    account_type,
    LAG(opened_date, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS prev_opened_date,
    RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS opened_date_rank
FROM accounts
WHERE customer_id IS NOT NULL;
