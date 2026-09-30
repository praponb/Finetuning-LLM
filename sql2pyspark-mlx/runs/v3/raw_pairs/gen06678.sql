SELECT hire_date FROM employees WHERE UPPER(email) LIKE '%RAC%' EXCEPT SELECT opened_date FROM accounts ORDER BY hire_date DESC
