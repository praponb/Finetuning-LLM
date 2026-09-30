SELECT
    d.id,
    d.dept_name,
    d.location,
    t.total_salary,
    t.pct_designer
FROM departments d
INNER JOIN (SELECT dept_id, SUM(salary) AS total_salary, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.total_salary >= 419000
