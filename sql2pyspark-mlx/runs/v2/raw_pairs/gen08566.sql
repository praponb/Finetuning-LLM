SELECT
    id,
    customer_id,
    status,
    opened_date,
    LEAD(opened_date, 1) OVER (PARTITION BY customer_id, status ORDER BY opened_date, id) AS next_opened_date,
    DENSE_RANK() OVER (PARTITION BY customer_id, status ORDER BY opened_date) AS dense_rnk
FROM accounts;
