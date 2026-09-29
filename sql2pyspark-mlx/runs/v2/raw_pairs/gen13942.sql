SELECT dept_id, COUNT(DISTINCT salary) unique_managers, COUNT(*) num_employees FROM employees WHERE hire_date >= '2025-07-08' AND hire_date BETWEEN '2024-06-01' AND '2025-06-12' GROUP BY dept_id;
