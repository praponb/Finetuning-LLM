SELECT id, country, credit_limit, RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_rank FROM customers
