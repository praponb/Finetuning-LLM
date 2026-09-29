SELECT id, country, age FROM customers WHERE signup_date BETWEEN '2024-08-01' AND '2025-06-01' EXCEPT SELECT id, country, age FROM customers WHERE signup_date BETWEEN '2023-07-01' AND '2024-12-01';
