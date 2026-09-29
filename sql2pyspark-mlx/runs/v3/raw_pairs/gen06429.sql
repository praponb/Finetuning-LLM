SELECT id, age, DENSE_RANK() OVER (ORDER BY age DESC) AS age_dense_rank FROM customers ORDER BY id DESC, age_dense_rank DESC;
