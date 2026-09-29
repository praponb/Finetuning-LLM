SELECT id, manager_id, salary, SUM(salary) OVER (PARTITION BY manager_id) AS manager_id_sum_salary FROM employees;
