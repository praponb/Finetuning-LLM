SELECT level, MAX(salary) max_salary FROM employees WHERE NOT is_remote AND salary BETWEEN 77000 AND 136000 GROUP BY level ORDER BY level ASC;
