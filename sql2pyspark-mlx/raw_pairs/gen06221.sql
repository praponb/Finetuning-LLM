SELECT
    e.level,
    e.manager_id,
    e.is_remote,
    d.budget
FROM employees e
JOIN departments d ON d.id = e.dept_id
WHERE d.budget < 3098000
ORDER BY manager_id DESC, d.budget
