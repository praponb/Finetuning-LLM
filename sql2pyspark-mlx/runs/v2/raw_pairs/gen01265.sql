SELECT salary, UPPER(TRIM(email)) AS email_normalized
FROM employees
