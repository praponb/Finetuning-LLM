SELECT * FROM (SELECT dept_name, location, budget FROM departments) PIVOT (AVG(budget) FOR location IN ('New York', 'London', 'Tokyo', 'Chiang Mai')) ORDER BY dept_name
