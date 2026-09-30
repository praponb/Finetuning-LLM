SELECT e.id, e.manager_id, e.email FROM employees e WHERE e.full_name IS NOT NULL AND e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name NOT IN ('HR', 'Engineering', 'Sales'));
