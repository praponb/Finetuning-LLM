SELECT employees.manager_id, employees.salary, departments.budget FROM employees LEFT JOIN departments ON employees.dept_id = departments.id
