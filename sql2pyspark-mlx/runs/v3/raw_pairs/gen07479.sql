SELECT *
FROM departments
WHERE dept_name IS NULL AND dept_name != 'Sales'
ORDER BY id DESC, dept_name
LIMIT 10
