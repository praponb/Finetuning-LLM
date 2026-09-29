WITH department_totals AS (
    SELECT dept_id, MAX(hire_date) AS last_hire_date, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    departments.dept_name,
    department_totals.last_hire_date,
    department_totals.avg_salary
FROM departments
INNER JOIN department_totals ON departments.id = department_totals.dept_id
WHERE department_totals.avg_salary >= 32;
