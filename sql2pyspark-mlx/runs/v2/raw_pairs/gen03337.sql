SELECT dept_name, budget, location
FROM departments
WHERE budget BETWEEN 1917000 AND 3653000 AND budget BETWEEN 1904000 AND 2220000
ORDER BY budget DESC, dept_name DESC;
