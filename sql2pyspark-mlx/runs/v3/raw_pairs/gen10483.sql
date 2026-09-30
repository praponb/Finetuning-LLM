SELECT manager_id, MAX(salary) AS max_salary, SUM(salary) AS total_salary FROM employees WHERE LENGTH(full_name) > 7 GROUP BY 1 HAVING SUM(salary) < 484000;
