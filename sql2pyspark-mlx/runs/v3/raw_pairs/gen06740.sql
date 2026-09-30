SELECT d.dept_name, t.row_count
FROM departments d
JOIN (SELECT dept_id, COUNT(*) AS row_count
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.row_count >= 8
ORDER BY row_count DESC;
