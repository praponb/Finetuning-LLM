SELECT dept_name, id FROM departments EXCEPT ALL SELECT dept_name, id FROM departments WHERE dept_name NOT IN ('Marketing', 'Engineering');
