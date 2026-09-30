SELECT
    id,
    customer_id,
    opened_date,
    status,
    NTILE(10) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS bucket,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date) AS opened_date_dense_rank
FROM accounts;
