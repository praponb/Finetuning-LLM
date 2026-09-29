SELECT
    e.id,
    e.full_name,
    e.is_remote,
    e.dept_id
FROM employees e
WHERE e.full_name LIKE 'bo%' AND e.dept_id IN (SELECT id FROM departments WHERE location NOT IN ('Bangkok', 'New York', 'London'));
