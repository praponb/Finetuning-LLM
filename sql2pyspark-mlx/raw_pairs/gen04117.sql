SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE location IS NULL) PIVOT (COUNT(budget) FOR dept_name IN ('Legal', 'Finance', 'Sales', 'Marketing')) ORDER BY location
