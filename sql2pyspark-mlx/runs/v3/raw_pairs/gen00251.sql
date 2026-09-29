WITH employees_by_department AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.budget, ebd.avg_salary
FROM departments d
LEFT OUTER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.avg_salary >= 113000
ORDER BY d.budget DESC;
