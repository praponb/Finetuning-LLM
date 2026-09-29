SELECT id, salary, email, level, SUBSTRING(email, 1, 4) AS email_prefix FROM employees;
