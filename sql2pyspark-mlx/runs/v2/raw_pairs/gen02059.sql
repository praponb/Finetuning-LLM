SELECT d.dept_name, t.level_list, t.l1_salary
FROM departments d
LEFT JOIN (SELECT dept_id, SORT_ARRAY(COLLECT_SET(level)) AS level_list, SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) AS l1_salary
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.l1_salary > 22
