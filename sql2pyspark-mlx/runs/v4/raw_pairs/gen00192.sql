SELECT *, ROUND(budget * 1.15, 2) AS adjusted_budget, LENGTH(dept_name) AS dept_name_length FROM departments WHERE location IN ('New York', 'Chiang Mai', 'Tokyo') ORDER BY id ASC;
