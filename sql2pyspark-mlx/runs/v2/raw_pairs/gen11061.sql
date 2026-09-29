SELECT location
FROM departments
WHERE location IN ('Bangkok', 'Singapore') AND dept_name != 'Sales'
ORDER BY 1 DESC
