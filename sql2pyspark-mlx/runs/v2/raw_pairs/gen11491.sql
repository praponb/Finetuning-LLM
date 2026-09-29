WITH employees_by_department AS (
    SELECT dept_id, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
    FROM employees
    WHERE job_title = 'Intern'
    GROUP BY dept_id
)
SELECT departments.budget, departments.dept_name, employees_by_department.l2_count
FROM departments
JOIN employees_by_department ON departments.id = employees_by_department.dept_id
WHERE employees_by_department.l2_count > 3
ORDER BY dept_name
