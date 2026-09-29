SELECT
    CASE WHEN departments.budget >= 3352000 THEN 'high' WHEN departments.budget >= 779000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN departments.dept_name IN ('Marketing', 'Engineering') THEN 'priority' ELSE 'other' END AS dept_name_group,
    MIN(employees.salary) AS min_salary,
    MAX(employees.salary) - MIN(employees.salary) AS salary_range
FROM employees
RIGHT JOIN departments ON departments.id = employees.dept_id
WHERE departments.budget BETWEEN 3465000 AND 3626000
GROUP BY CASE WHEN departments.budget >= 3352000 THEN 'high' WHEN departments.budget >= 779000 THEN 'mid' ELSE 'low' END, CASE WHEN departments.dept_name IN ('Marketing', 'Engineering') THEN 'priority' ELSE 'other' END
ORDER BY budget_bucket, salary_range;
