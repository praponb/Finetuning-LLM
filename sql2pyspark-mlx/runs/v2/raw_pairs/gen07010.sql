SELECT d.dept_name, d.location, t.avg_salary
FROM departments d
LEFT OUTER JOIN (SELECT dept_id, ROUND(AVG(salary), 2) avg_salary
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY dept_name;
