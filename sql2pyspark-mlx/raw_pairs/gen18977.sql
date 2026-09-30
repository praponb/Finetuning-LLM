SELECT dept_name, COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count
FROM departments
GROUP BY dept_name
ORDER BY dept_name, hr_count DESC;
