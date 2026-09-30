SELECT id, credit_limit, segment, name FROM customers WHERE credit_limit < (SELECT MIN(credit_limit) FROM customers) ORDER BY credit_limit DESC, id LIMIT 50;
