SELECT id, location, dept_name FROM departments WHERE location IS NOT NULL EXCEPT SELECT id, location, dept_name FROM departments WHERE dept_name IN ('HR', 'Finance');
