SELECT id, is_active, city, UPPER(name) AS name_upper FROM customers WHERE email LIKE '%@gmail.com';
