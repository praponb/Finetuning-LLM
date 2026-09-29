SELECT id, full_name, dept_id
FROM employees
WHERE salary <= 89000 AND salary BETWEEN 116000 AND 144000
ORDER BY full_name DESC
