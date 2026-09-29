SELECT id, city, age, COALESCE(credit_limit, 0) AS credit_limit_filled FROM customers WHERE segment = 'SMB' ORDER BY city, age DESC;
