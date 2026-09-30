SELECT id, country, age, DENSE_RANK() OVER (PARTITION BY country ORDER BY age) AS dense_rnk FROM customers ORDER BY id DESC
