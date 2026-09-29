WITH department_totals AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END) AS l3_count, SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, dt.l3_count, dt.engineer_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id;
