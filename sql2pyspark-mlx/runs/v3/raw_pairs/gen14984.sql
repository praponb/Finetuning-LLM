SELECT d.id, d.budget
FROM departments d
WHERE d.location IN ('Berlin', 'Chiang Mai') AND d.id IN (SELECT dept_id FROM employees)
