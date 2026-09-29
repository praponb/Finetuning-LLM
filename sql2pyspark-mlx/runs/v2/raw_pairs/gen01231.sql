SELECT
    d.id,
    d.budget,
    t.engineer_count,
    t.salary_range
FROM departments d
INNER JOIN (SELECT dept_id, COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY engineer_count, budget, id
LIMIT 1
