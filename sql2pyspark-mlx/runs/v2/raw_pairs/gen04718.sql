SELECT dept_id, l3_count, manager_count
FROM (
    SELECT dept_id, COUNT(CASE WHEN level = 'L3' THEN 1 END) l3_count, COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) manager_count
    FROM employees
    GROUP BY dept_id
) sub
WHERE l3_count > 4
ORDER BY l3_count, manager_count DESC, dept_id
LIMIT 5;
