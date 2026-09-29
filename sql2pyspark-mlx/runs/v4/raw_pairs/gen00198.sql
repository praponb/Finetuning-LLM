SELECT id, budget
FROM departments
WHERE location IS NOT NULL AND dept_name <> 'Marketing' AND dept_name IN ('Legal', 'HR')
ORDER BY id DESC;
