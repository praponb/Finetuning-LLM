SELECT id, country, credit_limit, ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 3) AS credit_limit_share FROM customers
