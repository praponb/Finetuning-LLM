SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name = 'Sales') PIVOT (AVG(budget) FOR location IN ('Singapore', 'New York')) ORDER BY dept_name
