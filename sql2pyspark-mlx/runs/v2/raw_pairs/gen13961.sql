SELECT id, segment, credit_limit, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS rn FROM customers
