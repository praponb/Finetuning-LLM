SELECT id, segment, credit_limit, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS credit_limit_rank FROM customers WHERE age != 41;
