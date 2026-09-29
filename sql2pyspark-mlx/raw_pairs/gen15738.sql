SELECT dept_id, email, is_remote
FROM employees
WHERE level IS NOT NULL AND (email NOT LIKE '%@mail.co.th' OR hire_date > '2023-05-17')
ORDER BY email, dept_id DESC
