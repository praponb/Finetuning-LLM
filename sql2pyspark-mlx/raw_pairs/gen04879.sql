SELECT hire_date FROM employees WHERE hire_date IS NOT NULL UNION SELECT opened_date FROM accounts WHERE opened_date IS NOT NULL;
