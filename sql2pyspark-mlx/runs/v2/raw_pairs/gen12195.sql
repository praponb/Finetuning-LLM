SELECT id, city, credit_limit, RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_rank FROM customers WHERE age BETWEEN 59 AND 62 ORDER BY credit_limit;
