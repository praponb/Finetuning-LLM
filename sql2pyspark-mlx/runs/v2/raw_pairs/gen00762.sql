SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE dept_name = 'Sales') PIVOT (COUNT(budget) FOR dept_name IN ('Engineering', 'Marketing')) ORDER BY location;
