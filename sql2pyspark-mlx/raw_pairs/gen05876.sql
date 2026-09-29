WITH department_totals AS (
    SELECT dept_id, MAX(hire_date) AS last_hire_date, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    dt.last_hire_date,
    dt.salary_range
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.salary_range > 37;
