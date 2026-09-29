SELECT id, country, credit_limit, RANK() OVER (PARTITION BY country ORDER BY credit_limit) AS rnk FROM customers ORDER BY id
