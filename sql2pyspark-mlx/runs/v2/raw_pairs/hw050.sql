SELECT id, credit_limit, NTILE(4) OVER (ORDER BY credit_limit DESC, id) AS quartile FROM customers;
