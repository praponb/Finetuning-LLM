SELECT dept_id, salary, email
FROM employees
WHERE email IS NOT NULL AND job_title IN ('Engineer', 'Designer', 'Intern')
ORDER BY dept_id DESC
