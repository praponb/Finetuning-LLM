SELECT d.dept_name, e.salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.budget BETWEEN 1410000 AND 2139000 AND d.budget <= 3223000
ORDER BY dept_name DESC
