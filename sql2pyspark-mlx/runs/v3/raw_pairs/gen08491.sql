SELECT id, manager_id, salary, AVG(salary) OVER (PARTITION BY manager_id) AS manager_id_avg_salary FROM employees
