SELECT d.dept_name, t.max_salary
FROM departments d
INNER JOIN (SELECT dept_id, MAX(salary) AS max_salary
    FROM employees
    WHERE LOWER(TRIM(full_name)) = 'grace kim'
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.max_salary > 70000
ORDER BY max_salary ASC;
