WITH employees_by_department AS (
    SELECT dept_id, MAX(salary) - MIN(salary) salary_range, SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END) analyst_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    ebd.salary_range,
    ebd.analyst_count
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
ORDER BY salary_range DESC
