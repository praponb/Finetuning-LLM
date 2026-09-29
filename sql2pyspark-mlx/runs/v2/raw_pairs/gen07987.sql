SELECT * FROM (SELECT id, city, credit_limit, name, DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS pos FROM customers) AS r WHERE pos <= 2;
