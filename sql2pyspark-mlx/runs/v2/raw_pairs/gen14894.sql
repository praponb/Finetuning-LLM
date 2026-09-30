SELECT id, city, name FROM customers WHERE credit_limit <= 15600 EXCEPT SELECT id, city, name FROM customers WHERE LOWER(TRIM(email)) = 'bob.smith@yahoo.com';
