WITH department_totals AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range, MAX(hire_date) AS latest_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, dt.salary_range, dt.latest_hire_date
FROM departments d
LEFT OUTER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.salary_range > 51
