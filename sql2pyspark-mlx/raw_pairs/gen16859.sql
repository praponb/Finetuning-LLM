SELECT id, city, credit_limit, DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS credit_limit_dense_rank FROM customers ORDER BY id DESC, 4 DESC
