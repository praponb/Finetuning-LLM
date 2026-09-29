SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name != 'Legal') PIVOT (AVG(budget) FOR location IN ('Berlin', 'Tokyo', 'Singapore')) ORDER BY dept_name;
