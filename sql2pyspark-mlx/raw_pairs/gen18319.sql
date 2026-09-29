SELECT * FROM (SELECT location, dept_name, budget FROM departments) PIVOT (MAX(budget) FOR dept_name IN ('HR', 'Engineering', 'Marketing')) ORDER BY location
