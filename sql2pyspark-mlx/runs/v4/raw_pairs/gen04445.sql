SELECT id, location FROM departments EXCEPT SELECT id, location FROM departments WHERE dept_name IN ('Engineering', 'Finance');
