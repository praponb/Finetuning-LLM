SELECT id, credit_limit, is_active FROM customers WHERE credit_limit >= (SELECT AVG(credit_limit) FROM customers WHERE credit_limit BETWEEN 10300 AND 12800)
