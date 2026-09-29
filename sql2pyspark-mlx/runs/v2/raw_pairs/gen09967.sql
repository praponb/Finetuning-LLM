SELECT d.id, d.budget
FROM departments d
WHERE d.location = 'Singapore' AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.job_title IN ('Analyst', 'Intern'))
ORDER BY d.id DESC
