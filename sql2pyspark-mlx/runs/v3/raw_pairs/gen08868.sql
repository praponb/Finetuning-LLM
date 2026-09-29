SELECT employees.id, employees.dept_id, employees.level, employees.hire_date, departments.location FROM employees LEFT JOIN departments ON employees.dept_id = departments.id;
