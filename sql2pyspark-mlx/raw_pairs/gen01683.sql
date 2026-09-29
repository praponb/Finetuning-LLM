WITH department_totals AS (
    SELECT dept_id, COUNT(DISTINCT salary) unique_managers
    FROM employees
    WHERE YEAR(hire_date) = 2025
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    departments.location,
    department_totals.unique_managers
FROM departments
INNER JOIN department_totals ON departments.id = department_totals.dept_id
WHERE department_totals.unique_managers > 9
ORDER BY 2 DESC;
