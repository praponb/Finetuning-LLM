SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) pos FROM customers WHERE signup_date <= '2024-04-17') r WHERE pos = 1 ORDER BY signup_date DESC;
