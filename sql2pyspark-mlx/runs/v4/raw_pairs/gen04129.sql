SELECT segment, age FROM customers WHERE name IS NOT NULL UNION SELECT segment, age FROM customers WHERE MONTH(signup_date) IN (8, 10, 12)
