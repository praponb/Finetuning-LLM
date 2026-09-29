SELECT email, city FROM customers WHERE country = 'SG' UNION SELECT email, city FROM customers WHERE credit_limit <= 11100 ORDER BY city
