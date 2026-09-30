SELECT
    e.id,
    e.is_remote,
    e.email,
    e.salary
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE budget < 1158000)
