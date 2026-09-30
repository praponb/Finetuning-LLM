WITH department_totals AS (
    SELECT dept_id, SUM(CASE WHEN job_title = 'Manager' THEN salary ELSE 0 END) AS manager_salary, MIN(hire_date) AS first_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    dt.manager_salary,
    dt.first_hire_date
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.manager_salary >= 43
ORDER BY manager_salary DESC;
