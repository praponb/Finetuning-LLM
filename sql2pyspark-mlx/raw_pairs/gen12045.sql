SELECT id, email, segment, LOWER(name) AS name_lower FROM customers WHERE age = 59 AND credit_limit <= 8900 ORDER BY id, email
