SELECT id, segment, credit_limit, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS rnk FROM customers
