SELECT id, country, age, DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS age_dense_rank FROM customers WHERE is_active = FALSE
