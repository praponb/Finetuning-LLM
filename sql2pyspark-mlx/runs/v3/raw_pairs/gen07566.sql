SELECT hire_month, max_salary FROM (SELECT MONTH(hire_date) hire_month, MAX(salary) max_salary FROM employees WHERE manager_id IS NULL GROUP BY MONTH(hire_date)) x WHERE max_salary < 146000
