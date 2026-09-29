SELECT e.id, e.dept_id, d.location FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.level NOT IN ('L3', 'L1', 'L2')
