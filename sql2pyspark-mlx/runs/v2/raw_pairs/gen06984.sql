SELECT ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2, MAX(salary) AS max_salary FROM employees;
