SELECT id, credit_limit, is_active FROM customers WHERE credit_limit >= (SELECT MAX(credit_limit) FROM customers WHERE credit_limit >= 11200) ORDER BY 2
