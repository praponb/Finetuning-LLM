SELECT id, is_active FROM customers WHERE segment IS NOT NULL AND credit_limit <= 4800 ORDER BY 1 DESC
