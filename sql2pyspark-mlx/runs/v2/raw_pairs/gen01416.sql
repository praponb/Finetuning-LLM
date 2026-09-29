SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget < 1413000) PIVOT (SUM(budget) FOR dept_name IN ('Legal', 'Sales')) ORDER BY location
