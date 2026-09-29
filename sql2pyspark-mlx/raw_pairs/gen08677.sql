SELECT
    id,
    country,
    credit_limit,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_dense_rank
FROM customers
ORDER BY credit_limit_dense_rank DESC, id;
