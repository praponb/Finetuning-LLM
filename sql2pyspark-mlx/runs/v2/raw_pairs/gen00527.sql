SELECT id, credit_limit, segment, country FROM customers WHERE credit_limit >= (SELECT MAX(credit_limit) FROM customers) ORDER BY credit_limit DESC;
