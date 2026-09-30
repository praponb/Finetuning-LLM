WITH department_totals AS (
    SELECT dept_id, MAX(hire_date) AS latest_hire_date
    FROM employees
    WHERE LOWER(TRIM(full_name)) = 'jack taylor'
    GROUP BY dept_id
)
SELECT d.id, d.dept_name, dt.latest_hire_date
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id;
