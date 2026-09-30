SELECT id, credit_limit, NTILE(2) OVER (ORDER BY credit_limit DESC, id) AS credit_limit_ntile FROM customers ORDER BY credit_limit_ntile
