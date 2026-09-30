SELECT id, city, credit_limit, age FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS rnk FROM customers) AS r WHERE rnk <= 1;
