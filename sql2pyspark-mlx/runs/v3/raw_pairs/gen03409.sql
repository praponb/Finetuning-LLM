SELECT id, city FROM customers WHERE LOWER(TRIM(email)) = 'emma@outlook.com' EXCEPT SELECT id, city FROM customers WHERE country IN ('JP', 'UK', 'DE');
