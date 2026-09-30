SELECT e.level, d.budget FROM employees e JOIN departments d ON d.id = e.dept_id WHERE d.dept_name != 'HR' ORDER BY budget DESC, e.level
