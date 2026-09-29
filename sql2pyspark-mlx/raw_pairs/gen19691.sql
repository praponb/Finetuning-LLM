SELECT employees.id, employees.is_remote, employees.dept_id, departments.budget FROM employees LEFT OUTER JOIN departments ON employees.dept_id = departments.id ORDER BY budget DESC, 1 ASC
