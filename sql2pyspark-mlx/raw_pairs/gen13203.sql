SELECT e.id, e.dept_id
FROM employees e
WHERE e.manager_id = 28 AND e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name IN ('HR', 'Engineering', 'Finance'))
ORDER BY 1 DESC;
