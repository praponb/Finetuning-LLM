SELECT opened_date FROM accounts WHERE customer_id = 21 UNION SELECT hire_date FROM employees WHERE hire_date < '2024-06-15' ORDER BY opened_date;
