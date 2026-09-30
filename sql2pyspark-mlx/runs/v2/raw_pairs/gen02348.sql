SELECT
    d.id,
    d.budget,
    d.dept_name,
    t.stddev_salary,
    t.total_salary
FROM departments d
JOIN (SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, SUM(salary) AS total_salary
    FROM employees
    WHERE email LIKE '%@startup.io'
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.stddev_salary >= 45;
