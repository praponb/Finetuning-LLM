SELECT d.location, t.l3_count FROM departments d LEFT JOIN (SELECT dept_id, COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count FROM employees GROUP BY dept_id) t ON d.id = t.dept_id;
