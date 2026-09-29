SELECT id, age FROM customers WHERE signup_date <= '2023-03-01' UNION SELECT id, age FROM customers WHERE LENGTH(name) > 14;
