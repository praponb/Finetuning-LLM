SELECT * FROM (SELECT location, dept_name, budget FROM departments) PIVOT (COUNT(budget) FOR dept_name IN ('Legal', 'Engineering', 'Finance', 'Sales')) ORDER BY location
