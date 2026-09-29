WITH department_totals AS (
    SELECT dept_id, SUM(salary) AS total_salary, MIN(hire_date) AS first_hire_date
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.budget,
    dt.total_salary,
    dt.first_hire_date
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.total_salary > 797000;
