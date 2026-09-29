SELECT location FROM departments WHERE dept_name NOT IN ('Engineering', 'Marketing', 'Legal') UNION ALL SELECT location FROM departments WHERE dept_name IN ('HR', 'Finance')
