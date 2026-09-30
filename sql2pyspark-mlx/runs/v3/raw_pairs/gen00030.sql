SELECT
    d.id,
    d.budget,
    t.last_hire_date,
    t.level_values
FROM departments d
LEFT JOIN (SELECT dept_id, MAX(hire_date) AS last_hire_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id;
