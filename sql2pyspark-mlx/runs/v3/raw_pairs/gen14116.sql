SELECT d.dept_name, COUNT(e.id) employee_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(e.level))) level_values
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
GROUP BY d.dept_name;
