SELECT * FROM (SELECT MONTH(hire_date) AS hire_month, level, salary FROM employees)
PIVOT (MAX(salary) FOR level IN ('L2', 'L5', 'L3', 'L1'))
ORDER BY hire_month;
