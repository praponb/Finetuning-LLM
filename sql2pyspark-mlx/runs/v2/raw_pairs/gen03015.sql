SELECT id, city, credit_limit, DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS dense_rnk FROM customers ORDER BY credit_limit DESC;
