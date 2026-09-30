SELECT
    d.id,
    d.budget,
    t.l5_count,
    t.l2_count
FROM departments d
LEFT JOIN (SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count, SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count
    FROM employees
    WHERE LOWER(TRIM(email)) = 'ivy@gmail.com'
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY budget;
