SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget >= 1744000) PIVOT (AVG(budget) FOR dept_name IN ('Legal', 'Marketing', 'Sales', 'Engineering')) ORDER BY location;
