WITH department_totals AS (
    SELECT dept_id, MIN(salary) AS min_salary
    FROM employees
    WHERE hire_date BETWEEN '2023-12-14' AND '2024-02-01'
    GROUP BY dept_id
)
SELECT d.budget, dt.min_salary
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.min_salary >= 62000
ORDER BY budget DESC;
