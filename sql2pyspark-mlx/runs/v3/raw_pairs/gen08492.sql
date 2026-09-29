SELECT dept_name, COUNT(DISTINCT budget) AS unique_dept_names, COUNT(*) AS department_count
FROM departments
GROUP BY dept_name
ORDER BY department_count DESC;
