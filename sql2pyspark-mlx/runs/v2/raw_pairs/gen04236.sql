SELECT id, credit_limit, country FROM customers WHERE credit_limit > (SELECT MAX(credit_limit) FROM customers) ORDER BY credit_limit, id LIMIT 10;
