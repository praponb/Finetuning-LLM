SELECT id, credit_limit, age FROM customers WHERE credit_limit > (SELECT MAX(credit_limit) FROM customers);
