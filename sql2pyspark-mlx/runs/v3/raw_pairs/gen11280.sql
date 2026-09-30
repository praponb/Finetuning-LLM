SELECT id, level, dept_id
FROM employees
WHERE level = 'L5'
EXCEPT
SELECT id, level, dept_id
FROM employees
WHERE level NOT IN ('L1', 'L4');
