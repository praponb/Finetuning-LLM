WITH department_totals AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT departments.budget, departments.dept_name, department_totals.salary_range
FROM departments
INNER JOIN department_totals ON departments.id = department_totals.dept_id
ORDER BY 2 DESC;
