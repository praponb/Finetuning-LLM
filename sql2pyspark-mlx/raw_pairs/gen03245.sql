SELECT id, email, hire_date
FROM employees
WHERE salary BETWEEN 80000 AND 137000 AND NOT is_remote AND LOWER(TRIM(email)) = 'bob.smith@yahoo.com';
