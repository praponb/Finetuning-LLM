SELECT e.id, e.job_title, e.full_name
FROM employees e
WHERE e.level IN ('L5', 'L3', 'L4') AND e.dept_id IN (SELECT id FROM departments WHERE dept_name IS NULL)
ORDER BY e.id DESC
