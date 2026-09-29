SELECT id, credit_limit, city FROM customers WHERE credit_limit > (SELECT AVG(credit_limit) FROM customers)
