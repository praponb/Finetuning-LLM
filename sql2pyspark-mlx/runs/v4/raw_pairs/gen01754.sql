SELECT
    departments.id,
    departments.location,
    t.max_salary,
    t.level_list
FROM departments
JOIN (SELECT dept_id, MAX(salary) AS max_salary, SORT_ARRAY(COLLECT_SET(level)) AS level_list
    FROM employees
    WHERE YEAR(hire_date) = 2024
    GROUP BY dept_id) t ON departments.id = t.dept_id
WHERE t.max_salary > 59000
ORDER BY id DESC;
