SELECT id, budget
FROM departments
WHERE budget >= (SELECT MAX(budget) FROM departments WHERE dept_name IN ('Legal', 'Marketing', 'Finance'))
ORDER BY id DESC
