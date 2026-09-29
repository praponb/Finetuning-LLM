SELECT city FROM customers WHERE segment IS NULL INTERSECT SELECT city FROM customers WHERE credit_limit <= 6600 ORDER BY city DESC;
