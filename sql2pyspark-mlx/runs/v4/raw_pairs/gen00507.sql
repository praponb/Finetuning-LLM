SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE location = 'London') PIVOT (COUNT(budget) FOR dept_name IN ('HR', 'Sales')) ORDER BY location;
