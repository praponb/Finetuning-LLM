SELECT id, location
FROM departments
WHERE location NOT IN ('Tokyo', 'Chiang Mai') AND dept_name = 'Marketing' AND dept_name IN ('Marketing', 'HR', 'Legal');
