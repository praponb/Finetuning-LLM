SELECT city FROM customers WHERE signup_date < '2024-11-09' INTERSECT SELECT city FROM customers WHERE segment IN ('SMB', 'Consumer')
