WITH department_summary AS (
    SELECT dept_id, MIN(hire_date) first_hire_date, COUNT(CASE WHEN level = 'L3' THEN 1 END) l3_count
    FROM employees
    WHERE salary >= 148000
    GROUP BY dept_id
)
SELECT d.dept_name, ds.first_hire_date, ds.l3_count
FROM departments d
LEFT JOIN department_summary ds ON d.id = ds.dept_id;
