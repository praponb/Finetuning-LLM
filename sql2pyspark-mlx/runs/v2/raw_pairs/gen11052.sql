SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name IN ('Finance', 'Sales', 'HR')) PIVOT (AVG(budget) FOR location IN ('Tokyo', 'Singapore')) ORDER BY dept_name
