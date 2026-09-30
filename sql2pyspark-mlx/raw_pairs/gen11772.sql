SELECT id, is_remote, email
FROM employees
WHERE hire_date BETWEEN '2023-08-30' AND '2024-06-01'
ORDER BY email DESC;
