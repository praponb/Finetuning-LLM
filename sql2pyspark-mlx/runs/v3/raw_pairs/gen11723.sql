SELECT
    departments.id,
    departments.budget,
    departments.location,
    departments.dept_name
FROM departments
WHERE departments.location IN ('Chiang Mai', 'London') AND EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id AND employees.manager_id IS NULL)
