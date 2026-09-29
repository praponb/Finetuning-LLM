SELECT id, city FROM customers WHERE email IS NULL UNION ALL SELECT id, city FROM customers WHERE credit_limit <= 11400 ORDER BY id
