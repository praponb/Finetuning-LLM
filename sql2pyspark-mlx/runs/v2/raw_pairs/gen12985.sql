SELECT
    id,
    city,
    age,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY age DESC) age_dense_rank
FROM customers;
