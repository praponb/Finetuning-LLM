SELECT id, dept_name, location
FROM departments
WHERE budget BETWEEN 2752000 AND 3568000
EXCEPT
SELECT id, dept_name, location
FROM departments
WHERE dept_name IN ('Sales', 'Engineering', 'Finance');
