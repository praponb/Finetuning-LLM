SELECT e.id, e.dept_id AS employee_dept_id, m.hire_date AS manager_hire_date FROM employees AS e JOIN employees AS m ON e.manager_id = m.id WHERE e.level NOT IN ('L1', 'L4');
